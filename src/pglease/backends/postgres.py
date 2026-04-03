"""PostgreSQL backend implementation for pglease."""

import logging
import threading
from datetime import datetime, timedelta
from typing import Optional

import psycopg2
from psycopg2 import sql
from psycopg2.extras import RealDictCursor

from ..backend import Backend
from ..exceptions import BackendError
from ..models import Lease, AcquisitionResult

logger = logging.getLogger(__name__)


class PostgresBackend(Backend):
    """
    PostgreSQL-based backend using a lease table.
    
    Uses row-level locking (SELECT FOR UPDATE) to ensure atomic operations
    and prevent race conditions.
    """
    
    TABLE_NAME = "pglease_leases"
    
    def __init__(self, connection_string: str, auto_initialize: bool = True):
        """
        Initialize PostgreSQL backend.
        
        Args:
            connection_string: PostgreSQL connection string
            auto_initialize: Automatically create table if needed
        """
        self.connection_string = connection_string
        self._conn: Optional[psycopg2.extensions.connection] = None
        self._lock = threading.Lock()  # guards self._conn across threads

        if auto_initialize:
            self.initialize()
    
    def _get_connection(self) -> psycopg2.extensions.connection:
        """Get or create database connection."""
        if self._conn is None or self._conn.closed:
            self._conn = psycopg2.connect(
                self.connection_string,
                cursor_factory=RealDictCursor
            )
            # Set reasonable defaults
            self._conn.autocommit = False
        return self._conn
    
    def initialize(self) -> None:
        """Create the lease table if it doesn't exist."""
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {self.TABLE_NAME} (
            task_name VARCHAR(255) PRIMARY KEY,
            owner_id VARCHAR(255) NOT NULL,
            acquired_at TIMESTAMP NOT NULL,
            expires_at TIMESTAMP NOT NULL,
            heartbeat_at TIMESTAMP NOT NULL
        );
        """
        create_index_sql = f"""
        CREATE INDEX IF NOT EXISTS idx_{self.TABLE_NAME}_expires_at
        ON {self.TABLE_NAME}(expires_at);
        """

        try:
            with self._lock:
                conn = self._get_connection()
                with conn.cursor() as cur:
                    cur.execute(create_table_sql)
                    cur.execute(create_index_sql)
                conn.commit()
            logger.info(f"Initialized {self.TABLE_NAME} table")
        except Exception as e:
            with self._lock:
                if self._conn:
                    self._conn.rollback()
            raise BackendError(f"Failed to initialize backend: {e}") from e
    
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
        now = datetime.utcnow()
        expires_at = now + timedelta(seconds=ttl)
        
        try:
            with self._lock:
                conn = self._get_connection()
                with conn.cursor() as cur:
                    # Try to lock existing row
                    cur.execute(
                        f"""
                        SELECT task_name, owner_id, acquired_at, expires_at, heartbeat_at
                        FROM {self.TABLE_NAME}
                        WHERE task_name = %s
                        FOR UPDATE
                        """,
                        (task_name,)
                    )

                    row = cur.fetchone()

                    if row is None:
                        # Lease doesn't exist - create it
                        cur.execute(
                            f"""
                            INSERT INTO {self.TABLE_NAME}
                            (task_name, owner_id, acquired_at, expires_at, heartbeat_at)
                            VALUES (%s, %s, %s, %s, %s)
                            """,
                            (task_name, owner_id, now, expires_at, now)
                        )
                        conn.commit()

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
                        current_expires_at = row["expires_at"]
                        current_owner = row["owner_id"]

                        # Check if we already own it
                        if current_owner == owner_id:
                            # Renew our own lease
                            cur.execute(
                                f"""
                                UPDATE {self.TABLE_NAME}
                                SET expires_at = %s, heartbeat_at = %s
                                WHERE task_name = %s
                                """,
                                (expires_at, now, task_name)
                            )
                            conn.commit()

                            lease = Lease(
                                task_name=task_name,
                                owner_id=owner_id,
                                acquired_at=row["acquired_at"],
                                expires_at=expires_at,
                                heartbeat_at=now,
                            )
                            logger.debug(f"Renewed lease for {task_name} by {owner_id}")
                            return AcquisitionResult.acquired(lease)

                        # Check if expired
                        if current_expires_at <= now:
                            # Take over expired lease
                            cur.execute(
                                f"""
                                UPDATE {self.TABLE_NAME}
                                SET owner_id = %s, acquired_at = %s, expires_at = %s, heartbeat_at = %s
                                WHERE task_name = %s
                                """,
                                (owner_id, now, expires_at, now, task_name)
                            )
                            conn.commit()

                            lease = Lease(
                                task_name=task_name,
                                owner_id=owner_id,
                                acquired_at=now,
                                expires_at=expires_at,
                                heartbeat_at=now,
                            )
                            logger.info(
                                f"Acquired expired lease for {task_name} "
                                f"(was owned by {current_owner})"
                            )
                            return AcquisitionResult.acquired(lease)

                        else:
                            # Lease is held by another owner
                            conn.rollback()
                            time_remaining = (current_expires_at - now).total_seconds()
                            reason = (
                                f"Lease held by {current_owner}, "
                                f"expires in {time_remaining:.1f}s"
                            )
                            logger.debug(f"Failed to acquire {task_name}: {reason}")
                            return AcquisitionResult.failed(reason)

        except Exception as e:
            with self._lock:
                if self._conn:
                    self._conn.rollback()
            raise BackendError(f"Failed to acquire lease: {e}") from e
    
    def release(self, task_name: str, owner_id: str) -> bool:
        """
        Release a lease if owned by the specified owner.
        
        Only deletes the lease if the owner matches.
        """
        try:
            with self._lock:
                conn = self._get_connection()
                with conn.cursor() as cur:
                    cur.execute(
                        f"""
                        DELETE FROM {self.TABLE_NAME}
                        WHERE task_name = %s AND owner_id = %s
                        """,
                        (task_name, owner_id)
                    )
                    deleted = cur.rowcount > 0
                conn.commit()

            if deleted:
                logger.info(f"Released lease for {task_name} by {owner_id}")
            else:
                logger.debug(f"No lease to release for {task_name} by {owner_id}")

            return deleted

        except Exception as e:
            with self._lock:
                if self._conn:
                    self._conn.rollback()
            raise BackendError(f"Failed to release lease: {e}") from e
    
    def heartbeat(self, task_name: str, owner_id: str, ttl: int) -> bool:
        """
        Renew a lease by extending its expiration time.
        
        Only updates if the lease is owned by the specified owner.
        """
        now = datetime.utcnow()
        expires_at = now + timedelta(seconds=ttl)
        
        try:
            with self._lock:
                conn = self._get_connection()
                with conn.cursor() as cur:
                    cur.execute(
                        f"""
                        UPDATE {self.TABLE_NAME}
                        SET expires_at = %s, heartbeat_at = %s
                        WHERE task_name = %s AND owner_id = %s
                        """,
                        (expires_at, now, task_name, owner_id)
                    )
                    updated = cur.rowcount > 0
                conn.commit()

            if updated:
                logger.debug(f"Heartbeat successful for {task_name} by {owner_id}")
            else:
                logger.warning(f"Heartbeat failed for {task_name} by {owner_id}")

            return updated

        except Exception as e:
            with self._lock:
                if self._conn:
                    self._conn.rollback()
            raise BackendError(f"Failed to send heartbeat: {e}") from e
    
    def get_lease(self, task_name: str) -> Optional[Lease]:
        """Get the current lease for a task."""
        try:
            with self._lock:
                conn = self._get_connection()
                with conn.cursor() as cur:
                    cur.execute(
                        f"""
                        SELECT task_name, owner_id, acquired_at, expires_at, heartbeat_at
                        FROM {self.TABLE_NAME}
                        WHERE task_name = %s
                        """,
                        (task_name,)
                    )
                    row = cur.fetchone()

            if row is None:
                return None

            return Lease(
                task_name=row["task_name"],
                owner_id=row["owner_id"],
                acquired_at=row["acquired_at"],
                expires_at=row["expires_at"],
                heartbeat_at=row["heartbeat_at"],
            )

        except Exception as e:
            raise BackendError(f"Failed to get lease: {e}") from e
    
    def close(self) -> None:
        """Close database connection."""
        with self._lock:
            if self._conn and not self._conn.closed:
                self._conn.close()
        logger.debug("Closed PostgreSQL connection")
