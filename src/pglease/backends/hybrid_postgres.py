"""
Hybrid PostgreSQL backend using both advisory locks and lease table.

Combines the benefits of both approaches:
- Advisory locks for immediate failover on connection loss
- Lease table for observability, metadata, and TTL safety net
"""

from __future__ import annotations

import hashlib
import logging

from ..exceptions import BackendError
from ..models import AcquisitionResult
from .postgres import PostgresBackend

logger = logging.getLogger(__name__)


class HybridPostgresBackend(PostgresBackend):
    """
    Hybrid backend combining advisory locks with lease table.

    Acquisition Flow:
    1. Try to acquire PostgreSQL advisory lock (fast, instant cleanup)
    2. If successful, check/update lease table (metadata, observability)
    3. If lease table update fails, release advisory lock

    Heartbeat Flow:
    1. Verify advisory lock still held (connection alive?)
    2. Update lease table expiry time
    3. If advisory lock lost, fail heartbeat

    Release Flow:
    1. Release advisory lock (immediate)
    2. Delete lease table entry (cleanup metadata)

    Benefits:
    - Instant failover (<1s) via advisory lock auto-release
    - Rich metadata and observability via lease table
    - Connection validation during heartbeat
    - Double safety: advisory lock + TTL
    """

    def __init__(
        self,
        connection_string: str,
        auto_initialize: bool = True,
        connect_timeout: int = 10,
        pool_size: int = 1,
    ):
        """Initialize hybrid backend.

        Args:
            connection_string: PostgreSQL connection string.
            auto_initialize: Automatically create the lease table if needed.
            connect_timeout: Seconds to wait for a connection before raising
                (default 10).  Forwarded to
                :class:`~pglease.backends.postgres.PostgresBackend`.
            pool_size: Maximum number of simultaneous database connections
                for lease-table operations (default 1).  Note that advisory
                lock operations always use the dedicated single persistent
                connection (``self._conn``) regardless of this setting,
                because advisory locks are session-scoped.
        """
        super().__init__(
            connection_string,
            auto_initialize=auto_initialize,
            connect_timeout=connect_timeout,
            pool_size=pool_size,
        )
        self._held_locks: dict[str, int] = {}  # task_name -> lock_id

    @staticmethod
    def _task_to_lock_id(task_name: str) -> int:
        """
        Convert task name to PostgreSQL advisory lock ID.

        Uses a deterministic SHA-256 hash to generate a consistent 64-bit integer
        for the task name across all processes and interpreter restarts.
        """
        # Use hashlib.sha256 for deterministic cross-process hashing.
        # Python's built-in hash() is randomised per-process (PYTHONHASHSEED)
        # and must NOT be used here.
        digest = hashlib.sha256(task_name.encode()).digest()
        return int.from_bytes(digest[:8], "big") & ((2**63) - 1)

    def _try_advisory_lock(self, task_name: str) -> bool:
        """
        Try to acquire PostgreSQL advisory lock.

        Uses pg_try_advisory_lock which:
        - Returns immediately (non-blocking)
        - Returns true if acquired, false if held by another session
        - Auto-releases on connection close/crash
        """
        lock_id = self._task_to_lock_id(task_name)

        try:
            with self._lock:
                conn = self._get_connection()
                with conn.cursor() as cur:
                    cur.execute("SELECT pg_try_advisory_lock(%s)", (lock_id,))
                    result = cur.fetchone()[0]
                conn.commit()

            if result:
                self._held_locks[task_name] = lock_id
                logger.debug(f"Acquired advisory lock for {task_name} (id={lock_id})")
            else:
                logger.debug(f"Advisory lock already held for {task_name} (id={lock_id})")

            return result

        except Exception as e:
            raise BackendError(f"Failed to acquire advisory lock: {e}") from e

    def _release_advisory_lock(self, task_name: str) -> bool:
        """
        Release PostgreSQL advisory lock.

        Returns true if lock was held and released, false otherwise.
        """
        lock_id = self._held_locks.get(task_name)
        if lock_id is None:
            return False

        try:
            with self._lock:
                conn = self._get_connection()
                with conn.cursor() as cur:
                    cur.execute("SELECT pg_advisory_unlock(%s)", (lock_id,))
                    result = cur.fetchone()[0]
                conn.commit()

            if result:
                del self._held_locks[task_name]
                logger.debug(f"Released advisory lock for {task_name} (id={lock_id})")

            return result

        except Exception as e:
            logger.error(f"Failed to release advisory lock: {e}")
            return False

    def _verify_advisory_lock(self, task_name: str) -> bool:
        """
        Verify we still hold the advisory lock.

        This checks connection health - if connection was lost,
        advisory lock is auto-released and this returns false.
        """
        lock_id = self._held_locks.get(task_name)
        if lock_id is None:
            return False

        try:
            # Check if we're in the list of locks held by our session.
            # pg_try_advisory_lock(bigint) stores the 64-bit key split across
            # classid (upper 32 bits) and objid (lower 32 bits), both as oid
            # (unsigned 32-bit).  Querying only objid = %s is wrong for any
            # lock_id whose upper 32 bits are non-zero.  Reconstruct the full
            # 64-bit key for comparison instead.
            with self._lock:
                conn = self._get_connection()
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT COUNT(*) > 0
                        FROM pg_locks
                        WHERE locktype = 'advisory'
                          AND (classid::bigint << 32) | objid::bigint = %s
                          AND pid = pg_backend_pid()
                    """,
                        (lock_id,),
                    )
                    result = cur.fetchone()[0]
                conn.commit()

            return result

        except Exception as e:
            logger.error(f"Failed to verify advisory lock: {e}")
            return False

    def acquire(self, task_name: str, owner_id: str, ttl: int) -> AcquisitionResult:
        """
        Acquire lease using hybrid approach.

        Steps:
        1. Try advisory lock (fast check)
        2. If acquired, update lease table (metadata)
        3. If lease table fails, release advisory lock
        """
        # Step 1: Try advisory lock first (instant feedback)
        if not self._try_advisory_lock(task_name):
            return AcquisitionResult.failed("Advisory lock held by another worker")

        # Step 2: Update lease table (inherits parent's atomic logic)
        try:
            result = super().acquire(task_name, owner_id, ttl)

            if not result.success:
                # Lease table says no - release advisory lock
                self._release_advisory_lock(task_name)

            return result

        except Exception:
            # Cleanup advisory lock on any error
            self._release_advisory_lock(task_name)
            raise

    def release(self, task_name: str, owner_id: str) -> bool:
        """
        Release lease using hybrid approach.

        Releases both advisory lock and lease table entry.
        """
        # Release advisory lock first (immediate)
        advisory_released = self._release_advisory_lock(task_name)

        # Release lease table entry
        table_released = super().release(task_name, owner_id)

        # Consider successful if either was released
        return advisory_released or table_released

    def heartbeat(self, task_name: str, owner_id: str, ttl: int) -> bool:
        """
        Renew lease with connection verification.

        Steps:
        1. Verify advisory lock still held (connection check)
        2. Update lease table expiry
        """
        # Verify we still hold the advisory lock
        if not self._verify_advisory_lock(task_name):
            logger.error(
                f"Heartbeat failed for {task_name}: advisory lock lost "
                "(connection may have been interrupted)"
            )
            # Clean up lease table since we lost the lock
            super().release(task_name, owner_id)
            return False

        # Update lease table expiry
        return super().heartbeat(task_name, owner_id, ttl)

    def close(self) -> None:
        """
        Close backend and release all advisory locks.
        """
        # Release all advisory locks
        for task_name in list(self._held_locks.keys()):
            try:
                self._release_advisory_lock(task_name)
            except Exception as e:
                logger.error(f"Error releasing advisory lock for {task_name}: {e}")

        # Call parent close
        super().close()
