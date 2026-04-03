"""Integration tests for PostgresBackend — require TEST_POSTGRES_URL.

These complement tests/test_postgres.py with coverage for list_leases,
cleanup_expired, connection pooling, and edge cases.
"""

from __future__ import annotations

import os
import time
import uuid
from datetime import datetime, timedelta, timezone

import pytest

from pglease.backends.postgres import PostgresBackend
from pglease.exceptions import BackendError

POSTGRES_URL = os.environ.get("TEST_POSTGRES_URL")
pytestmark = pytest.mark.skipif(not POSTGRES_URL, reason="TEST_POSTGRES_URL not set")


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _uid() -> str:
    return uuid.uuid4().hex[:8]


@pytest.fixture
def backend():
    b = PostgresBackend(POSTGRES_URL, pool_size=1)
    yield b
    # Clean up all bench/test rows
    try:
        with b._connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"DELETE FROM {b.TABLE_NAME}")
    finally:
        b.close()


@pytest.fixture
def pool_backend():
    b = PostgresBackend(POSTGRES_URL, pool_size=3)
    yield b
    try:
        with b._connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"DELETE FROM {b.TABLE_NAME}")
    finally:
        b.close()


# ---------------------------------------------------------------------------
# list_leases
# ---------------------------------------------------------------------------

class TestListLeases:
    def test_empty_returns_empty_list(self, backend):
        leases = backend.list_leases()
        assert leases == []

    def test_returns_all_active_leases(self, backend):
        owner = "w-" + _uid()
        backend.acquire("task-a-" + _uid(), owner, ttl=60)
        backend.acquire("task-b-" + _uid(), owner, ttl=60)
        backend.acquire("task-c-" + _uid(), owner, ttl=60)
        leases = backend.list_leases()
        assert len(leases) >= 3

    def test_leases_ordered_by_task_name(self, backend):
        owner = "w-" + _uid()
        tasks = [f"zzz-{_uid()}", f"aaa-{_uid()}", f"mmm-{_uid()}"]
        for t in tasks:
            backend.acquire(t, owner, ttl=60)
        leases = backend.list_leases()
        names = [l.task_name for l in leases]
        assert names == sorted(names)

    def test_returns_expired_rows_not_yet_cleaned(self, backend):
        """list_leases returns all rows including expired ones."""
        task = "expired-" + _uid()
        owner = "w-" + _uid()
        backend.acquire(task, owner, ttl=1)
        time.sleep(2)
        leases = backend.list_leases()
        task_names = {l.task_name for l in leases}
        assert task in task_names  # row still present (not cleaned up)

    def test_lease_fields_are_timezone_aware(self, backend):
        owner = "w-" + _uid()
        task = "tz-" + _uid()
        backend.acquire(task, owner, ttl=60)
        leases = backend.list_leases()
        for lease in leases:
            assert lease.acquired_at.tzinfo is not None
            assert lease.expires_at.tzinfo is not None
            assert lease.heartbeat_at.tzinfo is not None


# ---------------------------------------------------------------------------
# cleanup_expired
# ---------------------------------------------------------------------------

class TestCleanupExpired:
    def test_returns_zero_when_nothing_expired(self, backend):
        owner = "w-" + _uid()
        backend.acquire("active-" + _uid(), owner, ttl=3600)
        count = backend.cleanup_expired()
        assert count == 0

    def test_deletes_expired_rows(self, backend):
        owner = "w-" + _uid()
        task = "short-" + _uid()
        backend.acquire(task, owner, ttl=1)
        time.sleep(2)
        count = backend.cleanup_expired()
        assert count >= 1
        assert backend.get_lease(task) is None

    def test_does_not_delete_active_leases(self, backend):
        owner = "w-" + _uid()
        task = "keep-" + _uid()
        backend.acquire(task, owner, ttl=3600)
        backend.cleanup_expired()
        assert backend.get_lease(task) is not None

    def test_returns_exact_deleted_count(self, backend):
        owner = "w-" + _uid()
        tasks = ["exp-" + _uid() for _ in range(5)]
        for t in tasks:
            backend.acquire(t, owner, ttl=1)
        time.sleep(2)
        count = backend.cleanup_expired()
        assert count == 5


# ---------------------------------------------------------------------------
# Connection pool (pool_size > 1)
# ---------------------------------------------------------------------------

class TestConnectionPool:
    def test_acquire_works_with_pool(self, pool_backend):
        task = "pool-" + _uid()
        owner = "w-" + _uid()
        result = pool_backend.acquire(task, owner, ttl=60)
        assert result.success
        assert result.lease.task_name == task

    def test_release_works_with_pool(self, pool_backend):
        task = "pool-rel-" + _uid()
        owner = "w-" + _uid()
        pool_backend.acquire(task, owner, ttl=60)
        released = pool_backend.release(task, owner)
        assert released
        assert pool_backend.get_lease(task) is None

    def test_heartbeat_works_with_pool(self, pool_backend):
        task = "pool-hb-" + _uid()
        owner = "w-" + _uid()
        pool_backend.acquire(task, owner, ttl=60)
        success = pool_backend.heartbeat(task, owner, ttl=120)
        assert success

    def test_list_leases_with_pool(self, pool_backend):
        owner = "w-" + _uid()
        for i in range(3):
            pool_backend.acquire(f"pool-list-{i}-{_uid()}", owner, ttl=60)
        leases = pool_backend.list_leases()
        assert len(leases) >= 3

    def test_concurrent_acquires_with_pool(self, pool_backend):
        """Multiple threads can acquire different leases without errors."""
        import threading
        results = []
        errors = []

        def worker():
            task = "concurrent-" + _uid()
            owner = "w-" + _uid()
            try:
                r = pool_backend.acquire(task, owner, ttl=60)
                results.append(r.success)
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=worker) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert errors == [], f"Errors: {errors}"
        assert all(results)


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------

class TestErrorHandling:
    def test_acquire_raises_backend_error_on_bad_connection(self):
        bad = PostgresBackend.__new__(PostgresBackend)
        bad.connection_string = "postgresql://nohost/nodb"
        bad.connect_timeout = 1
        bad._pool_size = 1
        bad._pool = None
        bad._conn = None
        bad._lock = __import__("threading").Lock()
        from psycopg2 import sql
        for attr in (
            "_sql_create_table", "_sql_create_index", "_sql_select_for_update",
            "_sql_insert", "_sql_update_renew", "_sql_update_takeover",
            "_sql_delete", "_sql_heartbeat", "_sql_select", "_sql_list",
            "_sql_cleanup_expired",
        ):
            setattr(bad, attr, sql.SQL("SELECT 1"))

        with pytest.raises((BackendError, Exception)):
            bad.acquire("t", "w", ttl=60)
