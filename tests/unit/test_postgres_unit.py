"""Unit tests for PostgresBackend — psycopg2 is mocked, no DB required."""

from __future__ import annotations

import threading
from contextlib import contextmanager
from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest

from pglease.backends.postgres import PostgresBackend, _scrub_exc, _to_utc
from pglease.exceptions import BackendError

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _utcnow() -> datetime:
    return datetime.now(UTC)


def _make_row(
    task: str = "t",
    owner: str = "w",
    ttl: int = 60,
    expired: bool = False,
) -> dict:
    now = _utcnow()
    from datetime import timedelta

    if expired:
        expires_at = now - timedelta(seconds=10)
    else:
        expires_at = now + timedelta(seconds=ttl)
    return {
        "task_name": task,
        "owner_id": owner,
        "acquired_at": now,
        "expires_at": expires_at,
        "heartbeat_at": now,
    }


def _make_backend_no_init() -> PostgresBackend:
    """Create a PostgresBackend without connecting to Postgres."""
    backend = PostgresBackend.__new__(PostgresBackend)
    backend.connection_string = "postgresql://user:pass@localhost/testdb"
    backend.connect_timeout = 10
    backend._pool_size = 1
    backend._pool = None
    backend._conn = None
    backend._lock = threading.Lock()
    # Build SQL objects without connecting
    from psycopg2 import sql

    _tbl = sql.Identifier(PostgresBackend.TABLE_NAME)
    _idx = sql.Identifier(f"idx_{PostgresBackend.TABLE_NAME}_expires_at")
    backend._sql_create_table = sql.SQL("SELECT 1")
    backend._sql_create_index = sql.SQL("SELECT 1")
    backend._sql_select_for_update = sql.SQL("SELECT 1")
    backend._sql_insert = sql.SQL("SELECT 1")
    backend._sql_update_renew = sql.SQL("SELECT 1")
    backend._sql_update_takeover = sql.SQL("SELECT 1")
    backend._sql_delete = sql.SQL("SELECT 1")
    backend._sql_heartbeat = sql.SQL("SELECT 1")
    backend._sql_select = sql.SQL("SELECT 1")
    backend._sql_list = sql.SQL("SELECT 1")
    backend._sql_cleanup_expired = sql.SQL("SELECT 1")
    return backend


def _make_cursor(row=None, rows=None, rowcount: int = 1) -> MagicMock:
    cursor = MagicMock()
    cursor.__enter__ = lambda s: s
    cursor.__exit__ = MagicMock(return_value=False)
    cursor.fetchone.return_value = row
    cursor.fetchall.return_value = rows or []
    cursor.rowcount = rowcount
    return cursor


def _make_conn(cursor: MagicMock) -> MagicMock:
    conn = MagicMock()
    conn.cursor.return_value = cursor
    conn.closed = False
    conn.autocommit = False
    return conn


@contextmanager
def _patch_connection(backend: PostgresBackend, conn: MagicMock):
    """Patch backend._connection() to yield *conn* directly."""

    @contextmanager
    def fake_connection():
        yield conn

    with patch.object(backend, "_connection", fake_connection):
        yield


# ---------------------------------------------------------------------------
# _scrub_exc
# ---------------------------------------------------------------------------


class TestScrubExc:
    def test_redacts_url_password(self):
        exc = Exception("postgresql://user:SuperSecret@localhost/db")
        result = _scrub_exc(exc)
        assert "SuperSecret" not in result
        assert "***" in result

    def test_redacts_keyword_password(self):
        exc = Exception("FATAL: password=MyP@ssw0rd authentication failed")
        result = _scrub_exc(exc)
        assert "MyP@ssw0rd" not in result
        assert "***" in result

    def test_no_credentials_unchanged(self):
        exc = Exception("connection refused")
        assert _scrub_exc(exc) == "connection refused"


# ---------------------------------------------------------------------------
# _to_utc
# ---------------------------------------------------------------------------


class TestToUtc:
    def test_naive_becomes_utc(self):
        naive = datetime(2024, 1, 1, 12, 0, 0)
        result = _to_utc(naive)
        assert result.tzinfo == UTC
        assert result.year == 2024

    def test_aware_normalised_to_utc(self):
        from datetime import timedelta
        from datetime import timezone as tz

        eastern = tz(timedelta(hours=-5))
        aware = datetime(2024, 6, 1, 10, 0, 0, tzinfo=eastern)
        result = _to_utc(aware)
        assert result.tzinfo == UTC
        assert result.hour == 15  # 10 + 5 = 15 UTC


# ---------------------------------------------------------------------------
# __init__ validation
# ---------------------------------------------------------------------------


class TestInit:
    def test_pool_size_zero_raises(self):
        with pytest.raises(ValueError, match="pool_size"):
            PostgresBackend("postgresql://fake", auto_initialize=False, pool_size=0)

    def test_pool_size_negative_raises(self):
        with pytest.raises(ValueError):
            PostgresBackend("postgresql://fake", auto_initialize=False, pool_size=-1)


# ---------------------------------------------------------------------------
# initialize
# ---------------------------------------------------------------------------


class TestInitialize:
    def test_calls_create_table_and_index(self):
        backend = _make_backend_no_init()
        cursor = _make_cursor()
        conn = _make_conn(cursor)
        with _patch_connection(backend, conn):
            backend.initialize()
        assert cursor.execute.call_count == 2  # create_table + create_index

    def test_wraps_exception_in_backend_error(self):
        backend = _make_backend_no_init()
        cursor = MagicMock()
        cursor.__enter__ = lambda s: s
        cursor.__exit__ = MagicMock(return_value=False)
        cursor.execute.side_effect = Exception("boom")
        conn = _make_conn(cursor)
        with (
            _patch_connection(backend, conn),
            pytest.raises(BackendError, match="Failed to initialize"),
        ):
            backend.initialize()


# ---------------------------------------------------------------------------
# acquire
# ---------------------------------------------------------------------------


class TestAcquire:
    def test_invalid_ttl_raises_value_error(self):
        backend = _make_backend_no_init()
        with pytest.raises(ValueError, match="ttl"):
            backend.acquire("t", "w", ttl=0)

    def test_negative_ttl_raises_value_error(self):
        backend = _make_backend_no_init()
        with pytest.raises(ValueError):
            backend.acquire("t", "w", ttl=-1)

    def test_new_lease_inserts_and_returns_acquired(self):
        backend = _make_backend_no_init()
        cursor = _make_cursor(row=None)  # row=None → INSERT path
        conn = _make_conn(cursor)
        with _patch_connection(backend, conn):
            result = backend.acquire("task", "owner", ttl=60)
        assert result.success is True
        assert result.lease.task_name == "task"

    def test_renew_own_lease(self):
        backend = _make_backend_no_init()
        row = _make_row(task="task", owner="owner")
        cursor = _make_cursor(row=row)
        conn = _make_conn(cursor)
        with _patch_connection(backend, conn):
            result = backend.acquire("task", "owner", ttl=60)
        assert result.success is True

    def test_fails_when_held_by_other(self):
        backend = _make_backend_no_init()
        row = _make_row(task="task", owner="other-worker", ttl=60)  # not expired
        cursor = _make_cursor(row=row)
        conn = _make_conn(cursor)
        with _patch_connection(backend, conn):
            result = backend.acquire("task", "my-worker", ttl=60)
        assert result.success is False
        assert "held" in result.reason.lower() or "expires" in result.reason.lower()

    def test_takes_over_expired_lease(self):
        backend = _make_backend_no_init()
        row = _make_row(task="task", owner="dead-worker", expired=True)
        cursor = _make_cursor(row=row)
        conn = _make_conn(cursor)
        with _patch_connection(backend, conn):
            result = backend.acquire("task", "new-worker", ttl=60)
        assert result.success is True
        assert result.lease.owner_id == "new-worker"

    def test_wraps_exception_in_backend_error(self):
        backend = _make_backend_no_init()
        cursor = MagicMock()
        cursor.__enter__ = lambda s: s
        cursor.__exit__ = MagicMock(return_value=False)
        cursor.execute.side_effect = Exception("db gone")
        conn = _make_conn(cursor)
        with (
            _patch_connection(backend, conn),
            pytest.raises(BackendError, match="Failed to acquire"),
        ):
            backend.acquire("t", "w", ttl=60)


# ---------------------------------------------------------------------------
# release
# ---------------------------------------------------------------------------


class TestRelease:
    def test_returns_true_when_deleted(self):
        backend = _make_backend_no_init()
        cursor = _make_cursor(rowcount=1)
        conn = _make_conn(cursor)
        with _patch_connection(backend, conn):
            result = backend.release("task", "owner")
        assert result is True

    def test_returns_false_when_not_owner(self):
        backend = _make_backend_no_init()
        cursor = _make_cursor(rowcount=0)
        conn = _make_conn(cursor)
        with _patch_connection(backend, conn):
            result = backend.release("task", "wrong-owner")
        assert result is False

    def test_wraps_exception(self):
        backend = _make_backend_no_init()
        cursor = MagicMock()
        cursor.__enter__ = lambda s: s
        cursor.__exit__ = MagicMock(return_value=False)
        cursor.execute.side_effect = Exception("boom")
        conn = _make_conn(cursor)
        with (
            _patch_connection(backend, conn),
            pytest.raises(BackendError, match="Failed to release"),
        ):
            backend.release("t", "w")


# ---------------------------------------------------------------------------
# heartbeat
# ---------------------------------------------------------------------------


class TestHeartbeat:
    def test_returns_true_when_updated(self):
        backend = _make_backend_no_init()
        cursor = _make_cursor(rowcount=1)
        conn = _make_conn(cursor)
        with _patch_connection(backend, conn):
            result = backend.heartbeat("task", "owner", ttl=60)
        assert result is True

    def test_returns_false_when_not_owner(self):
        backend = _make_backend_no_init()
        cursor = _make_cursor(rowcount=0)
        conn = _make_conn(cursor)
        with _patch_connection(backend, conn):
            result = backend.heartbeat("task", "wrong-owner", ttl=60)
        assert result is False

    def test_invalid_ttl_raises(self):
        backend = _make_backend_no_init()
        with pytest.raises(ValueError):
            backend.heartbeat("t", "o", ttl=0)


# ---------------------------------------------------------------------------
# get_lease
# ---------------------------------------------------------------------------


class TestGetLease:
    def test_returns_none_when_no_row(self):
        backend = _make_backend_no_init()
        cursor = _make_cursor(row=None)
        conn = _make_conn(cursor)
        with _patch_connection(backend, conn):
            result = backend.get_lease("missing")
        assert result is None

    def test_returns_lease_when_row_exists(self):
        backend = _make_backend_no_init()
        row = _make_row(task="my-task", owner="w1")
        cursor = _make_cursor(row=row)
        conn = _make_conn(cursor)
        with _patch_connection(backend, conn):
            lease = backend.get_lease("my-task")
        assert lease is not None
        assert lease.task_name == "my-task"
        assert lease.owner_id == "w1"


# ---------------------------------------------------------------------------
# list_leases
# ---------------------------------------------------------------------------


class TestListLeases:
    def test_returns_empty_list(self):
        backend = _make_backend_no_init()
        cursor = _make_cursor(rows=[])
        conn = _make_conn(cursor)
        with _patch_connection(backend, conn):
            leases = backend.list_leases()
        assert leases == []

    def test_returns_hydrated_leases(self):
        backend = _make_backend_no_init()
        rows = [_make_row("a"), _make_row("b"), _make_row("c")]
        cursor = _make_cursor(rows=rows)
        conn = _make_conn(cursor)
        with _patch_connection(backend, conn):
            leases = backend.list_leases()
        assert len(leases) == 3
        assert leases[0].task_name == "a"


# ---------------------------------------------------------------------------
# cleanup_expired
# ---------------------------------------------------------------------------


class TestCleanupExpired:
    def test_returns_deleted_count(self):
        backend = _make_backend_no_init()
        cursor = _make_cursor(rowcount=7)
        conn = _make_conn(cursor)
        with _patch_connection(backend, conn):
            count = backend.cleanup_expired()
        assert count == 7

    def test_wraps_exception(self):
        backend = _make_backend_no_init()
        cursor = MagicMock()
        cursor.__enter__ = lambda s: s
        cursor.__exit__ = MagicMock(return_value=False)
        cursor.execute.side_effect = Exception("gone")
        conn = _make_conn(cursor)
        with _patch_connection(backend, conn), pytest.raises(BackendError, match="Failed to clean"):
            backend.cleanup_expired()


# ---------------------------------------------------------------------------
# close
# ---------------------------------------------------------------------------


class TestClose:
    def test_closes_single_connection(self):
        backend = _make_backend_no_init()
        mock_conn = MagicMock()
        mock_conn.closed = False
        backend._conn = mock_conn
        backend.close()
        mock_conn.close.assert_called_once()

    def test_close_when_no_connection_is_safe(self):
        backend = _make_backend_no_init()
        backend._conn = None
        backend.close()  # must not raise

    def test_close_already_closed_connection_is_safe(self):
        backend = _make_backend_no_init()
        mock_conn = MagicMock()
        mock_conn.closed = True  # already closed
        backend._conn = mock_conn
        backend.close()
        mock_conn.close.assert_not_called()
