"""PostgreSQL backend implementation for pglease."""

import logging
import re
import threading
from contextlib import contextmanager, suppress
from datetime import UTC, datetime, timedelta

import psycopg2
from psycopg2 import sql
from psycopg2.extras import RealDictCursor

from ..backend import Backend
from ..exceptions import BackendError
from ..models import AcquisitionResult, Lease

logger = logging.getLogger(__name__)


def _scrub_exc(exc: Exception) -> str:
    """Return str(exc) with any embedded credentials redacted.

    psycopg2 errors often embed the full DSN (host/password) in their
    message text.  This helper strips:
    - URL passwords:   postgresql://user:SECRET@host  →  postgresql://user:***@host
    - keyword values:  password=SECRET                →  password=***
    """
    msg = str(exc)
    # URL form: scheme://user:password@
    msg = re.sub(r"(://[^:@/]+:)[^@/]+((?:@|//))", r"\1***\2", msg)
    # Key=value form: password=<token>
    return re.sub(r"(?i)(password\s*=\s*)\S+", r"\1***", msg)


def _to_utc(dt: datetime) -> datetime:
    """Ensure a datetime is timezone-aware UTC.

    psycopg2 may return either naive datetimes (plain TIMESTAMP column) or
    timezone-aware UTC datetimes (TIMESTAMPTZ or server configured with
    timezone=UTC).  Normalise to aware so comparisons never raise TypeError.
    """
    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


class PostgresBackend(Backend):
    """
    PostgreSQL-based backend using a lease table.

    Uses row-level locking (SELECT FOR UPDATE) to ensure atomic operations
    and prevent race conditions.
    """

    TABLE_NAME = "pglease_leases"

    def __init__(
        self,
        connection_string: str,
        auto_initialize: bool = True,
        connect_timeout: int = 10,
        pool_size: int = 1,
    ):
        """
        Initialize PostgreSQL backend.

        Args:
            connection_string: PostgreSQL connection string
            auto_initialize: Automatically create table if needed
            connect_timeout: Seconds to wait for a connection before raising
                an error (default 10).  Set to 0 to disable the timeout.
            pool_size: Maximum number of simultaneous database connections
                (default 1).  When > 1, a
                ``psycopg2.pool.ThreadedConnectionPool`` is used so multiple
                threads can perform DB operations concurrently rather than
                serialising through a single connection.
        """
        if pool_size < 1:
            raise ValueError(f"pool_size must be ≥ 1, got {pool_size!r}")
        self.connection_string = connection_string
        self.connect_timeout = connect_timeout
        self._pool_size = pool_size
        self._conn: psycopg2.extensions.connection | None = None
        self._lock = threading.Lock()  # guards self._conn across threads

        # Optional connection pool (pool_size > 1)
        if pool_size > 1:
            from psycopg2 import pool as _pg_pool  # lazy import

            _kwargs = {"cursor_factory": RealDictCursor}
            if connect_timeout:
                _kwargs["connect_timeout"] = connect_timeout
            self._pool = _pg_pool.ThreadedConnectionPool(
                minconn=1,
                maxconn=pool_size,
                dsn=connection_string,
                **_kwargs,
            )
        else:
            self._pool = None

        # Pre-build all SQL query objects using psycopg2.sql so that
        # TABLE_NAME is always properly quoted as an identifier and never
        # vulnerable to SQL injection via subclass overrides.
        _tbl = sql.Identifier(self.TABLE_NAME)
        _idx = sql.Identifier(f"idx_{self.TABLE_NAME}_expires_at")

        self._sql_create_table = sql.SQL("""
            CREATE TABLE IF NOT EXISTS {} (
                task_name    VARCHAR(255) PRIMARY KEY,
                owner_id     VARCHAR(255) NOT NULL,
                acquired_at  TIMESTAMPTZ  NOT NULL,
                expires_at   TIMESTAMPTZ  NOT NULL,
                heartbeat_at TIMESTAMPTZ  NOT NULL
            )
        """).format(_tbl)

        self._sql_create_index = sql.SQL("CREATE INDEX IF NOT EXISTS {} ON {}(expires_at)").format(
            _idx, _tbl
        )

        self._sql_select_for_update = sql.SQL("""
            SELECT task_name, owner_id, acquired_at, expires_at, heartbeat_at
            FROM {}
            WHERE task_name = %s
            FOR UPDATE
        """).format(_tbl)

        self._sql_insert = sql.SQL("""
            INSERT INTO {} (task_name, owner_id, acquired_at, expires_at, heartbeat_at)
            VALUES (%s, %s, %s, %s, %s)
        """).format(_tbl)

        self._sql_update_renew = sql.SQL("""
            UPDATE {} SET expires_at = %s, heartbeat_at = %s
            WHERE task_name = %s
        """).format(_tbl)

        self._sql_update_takeover = sql.SQL("""
            UPDATE {}
            SET owner_id = %s, acquired_at = %s, expires_at = %s, heartbeat_at = %s
            WHERE task_name = %s
        """).format(_tbl)

        self._sql_delete = sql.SQL("""
            DELETE FROM {}
            WHERE task_name = %s AND owner_id = %s
        """).format(_tbl)

        self._sql_heartbeat = sql.SQL("""
            UPDATE {} SET expires_at = %s, heartbeat_at = %s
            WHERE task_name = %s AND owner_id = %s
        """).format(_tbl)

        self._sql_select = sql.SQL("""
            SELECT task_name, owner_id, acquired_at, expires_at, heartbeat_at
            FROM {}
            WHERE task_name = %s
        """).format(_tbl)

        self._sql_list = sql.SQL("""
            SELECT task_name, owner_id, acquired_at, expires_at, heartbeat_at
            FROM {}
            ORDER BY task_name
        """).format(_tbl)

        self._sql_cleanup_expired = sql.SQL("""
            DELETE FROM {}
            WHERE expires_at < %s
        """).format(_tbl)

        if auto_initialize:
            self.initialize()

    def _get_connection(self) -> psycopg2.extensions.connection:
        """Get or create the single persistent connection (single-connection path).

        Also used by :class:`HybridPostgresBackend` for advisory-lock
        operations that must stay on the same session.
        Always called with ``self._lock`` held.
        """
        if self._conn is None or self._conn.closed:
            kwargs = {"cursor_factory": RealDictCursor}
            if self.connect_timeout:
                kwargs["connect_timeout"] = self.connect_timeout
            self._conn = psycopg2.connect(self.connection_string, **kwargs)
            # Set reasonable defaults
            self._conn.autocommit = False
        return self._conn

    @contextmanager
    def _connection(self):
        """Acquire a DB connection, yield it, then commit or rollback.

        On clean exit the connection is automatically committed, so callers
        do **not** need to call ``conn.commit()`` themselves.  On exception
        the transaction is rolled back before re-raising.

        When *pool_size > 1*: borrows a connection from the
        ``ThreadedConnectionPool`` and returns it to the pool after the
        block regardless of success or failure.

        When *pool_size == 1*: acquires ``self._lock`` and uses the single
        persistent connection.
        """
        if self._pool is not None:
            conn = self._pool.getconn()
            try:
                yield conn
                conn.commit()
            except Exception:
                with suppress(Exception):
                    conn.rollback()
                raise
            finally:
                self._pool.putconn(conn)
        else:
            with self._lock:
                conn = self._get_connection()
                try:
                    yield conn
                    conn.commit()
                except Exception:
                    with suppress(Exception):
                        conn.rollback()
                    raise

    def initialize(self) -> None:
        """Create the lease table if it doesn't exist."""
        try:
            with self._connection() as conn, conn.cursor() as cur:
                cur.execute(self._sql_create_table)
                cur.execute(self._sql_create_index)
            logger.info(f"Initialized {self.TABLE_NAME} table")
        except BackendError:
            raise
        except Exception as e:
            raise BackendError(f"Failed to initialize backend: {_scrub_exc(e)}") from e

    def acquire(self, task_name: str, owner_id: str, ttl: int) -> AcquisitionResult:
        """
        Acquire a lease using atomic row-level locking.

        Algorithm:
        1. Start transaction
        2. Lock the row (SELECT FOR UPDATE) or create if doesn't exist
        3. Check if lease is available (expired or non-existent)
        4. If available, acquire/update the lease
        5. Commit transaction
        """
        if ttl <= 0:
            raise ValueError(f"ttl must be a positive integer, got {ttl!r}")
        now = datetime.now(UTC)
        expires_at = now + timedelta(seconds=ttl)

        try:
            with self._connection() as conn, conn.cursor() as cur:
                # Try to lock existing row
                cur.execute(self._sql_select_for_update, (task_name,))
                row = cur.fetchone()

                if row is None:
                    # Lease doesn't exist - create it
                    cur.execute(self._sql_insert, (task_name, owner_id, now, expires_at, now))
                    lease = Lease(
                        task_name=task_name,
                        owner_id=owner_id,
                        acquired_at=now,
                        expires_at=expires_at,
                        heartbeat_at=now,
                    )
                    logger.info(f"Acquired new lease for {task_name} by {owner_id}")
                    return AcquisitionResult.acquired(lease)

                else:
                    # Lease exists - check if available
                    current_expires_at = _to_utc(row["expires_at"])
                    current_owner = row["owner_id"]

                    # Check if we already own it
                    if current_owner == owner_id:
                        # Renew our own lease
                        cur.execute(self._sql_update_renew, (expires_at, now, task_name))
                        lease = Lease(
                            task_name=task_name,
                            owner_id=owner_id,
                            acquired_at=_to_utc(row["acquired_at"]),
                            expires_at=expires_at,
                            heartbeat_at=now,
                        )
                        logger.debug(f"Renewed lease for {task_name} by {owner_id}")
                        return AcquisitionResult.acquired(lease)

                    # Check if expired
                    if current_expires_at <= now:
                        # Take over expired lease
                        cur.execute(
                            self._sql_update_takeover, (owner_id, now, expires_at, now, task_name)
                        )
                        lease = Lease(
                            task_name=task_name,
                            owner_id=owner_id,
                            acquired_at=now,
                            expires_at=expires_at,
                            heartbeat_at=now,
                        )
                        logger.info(
                            f"Acquired expired lease for {task_name} (was owned by {current_owner})"
                        )
                        return AcquisitionResult.acquired(lease)

                    else:
                        # Lease is held by another owner
                        time_remaining = (current_expires_at - now).total_seconds()
                        reason = f"Lease already held, expires in {time_remaining:.1f}s"
                        logger.debug(
                            f"Failed to acquire {task_name}: held by "
                            f"{current_owner}, {time_remaining:.1f}s remaining"
                        )
                        return AcquisitionResult.failed(reason)

        except (BackendError, ValueError):
            raise
        except Exception as e:
            raise BackendError(f"Failed to acquire lease: {_scrub_exc(e)}") from e

    def release(self, task_name: str, owner_id: str) -> bool:
        """
        Release a lease if owned by the specified owner.

        Only deletes the lease if the owner matches.
        """
        try:
            with self._connection() as conn, conn.cursor() as cur:
                cur.execute(self._sql_delete, (task_name, owner_id))
                deleted = cur.rowcount > 0

            if deleted:
                logger.info(f"Released lease for {task_name} by {owner_id}")
            else:
                logger.debug(f"No lease to release for {task_name} by {owner_id}")

            return deleted

        except BackendError:
            raise
        except Exception as e:
            raise BackendError(f"Failed to release lease: {_scrub_exc(e)}") from e

    def heartbeat(self, task_name: str, owner_id: str, ttl: int) -> bool:
        """
        Renew a lease by extending its expiration time.

        Only updates if the lease is owned by the specified owner.
        """
        if ttl <= 0:
            raise ValueError(f"ttl must be a positive integer, got {ttl!r}")
        now = datetime.now(UTC)
        expires_at = now + timedelta(seconds=ttl)

        try:
            with self._connection() as conn, conn.cursor() as cur:
                cur.execute(self._sql_heartbeat, (expires_at, now, task_name, owner_id))
                updated = cur.rowcount > 0

            if updated:
                logger.debug(f"Heartbeat successful for {task_name} by {owner_id}")
            else:
                logger.warning(f"Heartbeat failed for {task_name} by {owner_id}")

            return updated

        except BackendError:
            raise
        except Exception as e:
            raise BackendError(f"Failed to send heartbeat: {_scrub_exc(e)}") from e

    def get_lease(self, task_name: str) -> Lease | None:
        """Get the current lease for a task."""
        try:
            with self._connection() as conn, conn.cursor() as cur:
                cur.execute(self._sql_select, (task_name,))
                row = cur.fetchone()

            if row is None:
                return None

            return Lease(
                task_name=row["task_name"],
                owner_id=row["owner_id"],
                acquired_at=_to_utc(row["acquired_at"]),
                expires_at=_to_utc(row["expires_at"]),
                heartbeat_at=_to_utc(row["heartbeat_at"]),
            )

        except Exception as e:
            raise BackendError(f"Failed to get lease: {_scrub_exc(e)}") from e

    def list_leases(self) -> list[Lease]:
        """Return all leases currently in the store."""
        try:
            with self._connection() as conn, conn.cursor() as cur:
                cur.execute(self._sql_list)
                rows = cur.fetchall()

            return [
                Lease(
                    task_name=row["task_name"],
                    owner_id=row["owner_id"],
                    acquired_at=_to_utc(row["acquired_at"]),
                    expires_at=_to_utc(row["expires_at"]),
                    heartbeat_at=_to_utc(row["heartbeat_at"]),
                )
                for row in rows
            ]

        except Exception as e:
            raise BackendError(f"Failed to list leases: {_scrub_exc(e)}") from e

    def cleanup_expired(self) -> int:
        """Delete expired lease rows and return the number removed."""
        now = datetime.now(UTC)
        try:
            with self._connection() as conn, conn.cursor() as cur:
                cur.execute(self._sql_cleanup_expired, (now,))
                deleted = cur.rowcount

            if deleted:
                logger.info(f"Cleaned up {deleted} expired lease(s)")
            return deleted

        except BackendError:
            raise
        except Exception as e:
            raise BackendError(f"Failed to clean up expired leases: {_scrub_exc(e)}") from e

    def close(self) -> None:
        """Close database connection(s)."""
        if self._pool is not None:
            self._pool.closeall()
        with self._lock:
            if self._conn and not self._conn.closed:
                self._conn.close()
        logger.debug("Closed PostgreSQL connection(s)")
