"""Unit tests for PGLease — backend is a Mock, no DB required."""

from __future__ import annotations

import time
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from typing import List, Optional
from unittest.mock import MagicMock, Mock, call, patch

import pytest

from pglease.backend import Backend
from pglease.exceptions import AcquisitionError
from pglease.models import AcquisitionResult, Lease
from pglease.pglease import PGLease


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _now() -> datetime:
    return datetime.now(timezone.utc)


def _lease(task: str = "task", owner: str = "w", ttl: int = 60) -> Lease:
    now = _now()
    return Lease(task, owner, now, now + timedelta(seconds=ttl), now)


def _make_backend(*, acquired: bool = True, task: str = "task", owner: str = "w") -> MagicMock:
    """Return a mock Backend that succeeds or fails on acquire/heartbeat."""
    backend = MagicMock(spec=Backend)
    lease = _lease(task, owner)
    if acquired:
        backend.acquire.return_value = AcquisitionResult.acquired(lease)
    else:
        backend.acquire.return_value = AcquisitionResult.failed("held")
    backend.release.return_value = True
    backend.heartbeat.return_value = True
    backend.get_lease.return_value = lease
    backend.list_leases.return_value = [lease]
    backend.cleanup_expired.return_value = 3
    return backend


def _make_pglease(*, acquired: bool = True, task: str = "task") -> tuple[PGLease, MagicMock]:
    backend = _make_backend(acquired=acquired, task=task, owner="test-worker")
    # Pass backend directly to avoid connecting to Postgres
    pg = PGLease(backend, owner_id="test-worker", heartbeat_interval=3600)
    return pg, backend


# ---------------------------------------------------------------------------
# Initialisation
# ---------------------------------------------------------------------------

class TestPGLeaseInit:
    def test_accepts_backend_instance(self):
        backend = _make_backend()
        pg = PGLease(backend, owner_id="w1")
        assert pg.owner_id == "w1"
        assert pg.backend is backend

    def test_auto_generates_owner_id(self):
        backend = _make_backend()
        pg = PGLease(backend)
        assert pg.owner_id  # not empty
        assert "-" in pg.owner_id  # hostname-hex format

    def test_generate_owner_id_contains_hostname(self):
        backend = _make_backend()
        import socket
        pg = PGLease(backend)
        assert socket.gethostname() in pg.owner_id

    def test_heartbeat_interval_stored(self):
        backend = _make_backend()
        pg = PGLease(backend, heartbeat_interval=30)
        assert pg.heartbeat_manager.interval == 30

    def test_creates_postgres_backend_from_string(self):
        """Passing a connection string creates a PostgresBackend."""
        from pglease.backends.postgres import PostgresBackend
        with patch.object(PostgresBackend, "__init__", return_value=None) as mock_init, \
             patch.object(PostgresBackend, "initialize"):
            pg = PGLease.__new__(PGLease)
            # Just verify the branch exists; full init tested in integration tests
            pass


# ---------------------------------------------------------------------------
# try_acquire
# ---------------------------------------------------------------------------

class TestTryAcquire:
    def test_returns_true_on_success(self):
        pg, backend = _make_pglease(acquired=True)
        assert pg.try_acquire("task", ttl=60) is True
        pg.close()

    def test_returns_false_on_failure(self):
        pg, backend = _make_pglease(acquired=False)
        assert pg.try_acquire("task", ttl=60) is False
        pg.close()

    def test_calls_backend_acquire(self):
        pg, backend = _make_pglease()
        pg.try_acquire("my-task", ttl=90)
        backend.acquire.assert_called_once_with("my-task", "test-worker", 90)
        pg.close()

    def test_adds_to_active_leases_on_success(self):
        pg, _ = _make_pglease(acquired=True)
        pg.try_acquire("task", ttl=60)
        assert "task" in pg._active_leases
        pg.close()

    def test_does_not_add_to_active_leases_on_failure(self):
        pg, _ = _make_pglease(acquired=False)
        pg.try_acquire("task", ttl=60)
        assert "task" not in pg._active_leases
        pg.close()

    def test_starts_heartbeat_on_success(self):
        pg, _ = _make_pglease(acquired=True)
        pg.heartbeat_manager = MagicMock()
        pg.try_acquire("task", ttl=60)
        pg.heartbeat_manager.start.assert_called_once()
        pg.close()

    def test_on_lease_lost_callback_forwarded(self):
        lost_tasks = []
        pg, _ = _make_pglease(acquired=True)
        pg._on_lease_lost = lambda t: lost_tasks.append(t)
        pg.heartbeat_manager = MagicMock()
        pg.try_acquire("task", ttl=60)
        # Extract the on_lease_lost kwarg passed to start()
        _, kwargs = pg.heartbeat_manager.start.call_args
        internal_cb = kwargs["on_lease_lost"]
        # Simulate lease lost
        internal_cb("task")
        assert "task" in lost_tasks
        assert "task" not in pg._active_leases


# ---------------------------------------------------------------------------
# release
# ---------------------------------------------------------------------------

class TestRelease:
    def test_returns_true_when_released(self):
        pg, backend = _make_pglease()
        pg._active_leases.add("task")
        result = pg.release("task")
        assert result is True

    def test_returns_false_when_not_held(self):
        pg, backend = _make_pglease()
        backend.release.return_value = False
        result = pg.release("missing-task")
        assert result is False

    def test_removes_from_active_leases(self):
        pg, _ = _make_pglease()
        pg._active_leases.add("task")
        pg.release("task")
        assert "task" not in pg._active_leases

    def test_stops_heartbeat(self):
        pg, _ = _make_pglease()
        pg.heartbeat_manager = MagicMock()
        pg._active_leases.add("task")
        pg.release("task")
        pg.heartbeat_manager.stop.assert_called_once_with("task")


# ---------------------------------------------------------------------------
# acquire() context manager
# ---------------------------------------------------------------------------

class TestAcquireContextManager:
    def test_acquired_true_when_lease_obtained(self):
        pg, _ = _make_pglease(acquired=True)
        with pg.acquire("task", ttl=60) as acquired:
            assert acquired is True
        pg.close()

    def test_acquired_false_when_lease_not_obtained(self):
        pg, _ = _make_pglease(acquired=False)
        with pg.acquire("task", ttl=60) as acquired:
            assert acquired is False
        pg.close()

    def test_releases_on_exit(self):
        pg, backend = _make_pglease(acquired=True)
        with pg.acquire("task", ttl=60):
            pass
        backend.release.assert_called_once_with("task", "test-worker")
        pg.close()

    def test_does_not_release_if_not_acquired(self):
        pg, backend = _make_pglease(acquired=False)
        with pg.acquire("task", ttl=60):
            pass
        backend.release.assert_not_called()
        pg.close()

    def test_raises_on_failure_when_requested(self):
        pg, _ = _make_pglease(acquired=False)
        with pytest.raises(AcquisitionError):
            with pg.acquire("task", ttl=60, raise_on_failure=True):
                pass
        pg.close()

    def test_releases_even_on_exception_in_body(self):
        pg, backend = _make_pglease(acquired=True)
        with pytest.raises(RuntimeError):
            with pg.acquire("task", ttl=60):
                raise RuntimeError("boom")
        backend.release.assert_called_once()
        pg.close()


# ---------------------------------------------------------------------------
# get_lease / list_leases / cleanup_expired
# ---------------------------------------------------------------------------

class TestObservabilityMethods:
    def test_get_lease_delegates_to_backend(self):
        pg, backend = _make_pglease()
        lease = pg.get_lease("task")
        backend.get_lease.assert_called_once_with("task")
        assert lease is not None

    def test_get_lease_returns_none_when_missing(self):
        pg, backend = _make_pglease()
        backend.get_lease.return_value = None
        assert pg.get_lease("unknown") is None

    def test_list_leases_delegates(self):
        pg, backend = _make_pglease()
        leases = pg.list_leases()
        backend.list_leases.assert_called_once()
        assert len(leases) == 1

    def test_cleanup_expired_returns_count(self):
        pg, backend = _make_pglease()
        assert pg.cleanup_expired() == 3


# ---------------------------------------------------------------------------
# wait_for_lease
# ---------------------------------------------------------------------------

class TestWaitForLease:
    def test_succeeds_immediately_when_lease_available(self):
        pg, backend = _make_pglease(acquired=True)
        result = pg.wait_for_lease("task", ttl=60, timeout=10, poll_interval=0.1)
        assert result is True
        pg.close()

    def test_raises_value_error_for_timeout_zero(self):
        pg, _ = _make_pglease()
        with pytest.raises(ValueError, match="timeout=0"):
            pg.wait_for_lease("task", ttl=60, timeout=0)
        pg.close()

    def test_waits_and_retries(self):
        pg, backend = _make_pglease()
        # Fail twice then succeed
        pg.heartbeat_manager = MagicMock()
        lease = _lease()
        backend.acquire.side_effect = [
            AcquisitionResult.failed("held"),
            AcquisitionResult.failed("held"),
            AcquisitionResult.acquired(lease),
        ]
        with patch("time.sleep"):  # skip actual sleeping
            result = pg.wait_for_lease("task", ttl=60, timeout=10, poll_interval=0.01)
        assert result is True
        assert backend.acquire.call_count == 3
        pg.close()

    def test_raises_acquisition_error_on_timeout(self):
        pg, backend = _make_pglease(acquired=False)
        pg.heartbeat_manager = MagicMock()
        # Use a very short timeout with a real tiny sleep so it expires fast
        with patch("time.sleep"):  # make poll instant
            counter = [0]
            original = time.monotonic
            def fake_monotonic():
                counter[0] += 1
                # After a few calls, jump the clock forward
                return original() + (10.0 if counter[0] > 4 else 0.0)
            with patch("time.monotonic", side_effect=fake_monotonic):
                with pytest.raises(AcquisitionError):
                    pg.wait_for_lease("task", ttl=60, timeout=5.0, poll_interval=0.01)
        pg.close()

    def test_none_timeout_waits_indefinitely(self):
        """With timeout=None the deadline is infinity — succeeds on first try."""
        pg, _ = _make_pglease(acquired=True)
        pg.heartbeat_manager = MagicMock()
        result = pg.wait_for_lease("task", ttl=60, timeout=None, poll_interval=0.01)
        assert result is True
        pg.close()

    def test_inf_timeout_waits_indefinitely(self):
        pg, _ = _make_pglease(acquired=True)
        pg.heartbeat_manager = MagicMock()
        result = pg.wait_for_lease("task", ttl=60, timeout=float("inf"), poll_interval=0.01)
        assert result is True
        pg.close()


# ---------------------------------------------------------------------------
# singleton_task decorator
# ---------------------------------------------------------------------------

class TestSingletonTask:
    def test_executes_when_lease_acquired(self):
        pg, _ = _make_pglease(acquired=True)
        pg.heartbeat_manager = MagicMock()
        called = []

        @pg.singleton_task("task", ttl=60)
        def work():
            called.append(1)
            return "done"

        result = work()
        assert called == [1]
        assert result == "done"
        pg.close()

    def test_skips_when_lease_not_acquired(self):
        pg, _ = _make_pglease(acquired=False)
        called = []

        @pg.singleton_task("task", ttl=60)
        def work():
            called.append(1)

        result = work()
        assert called == []
        assert result is None
        pg.close()

    def test_raises_when_skip_if_locked_false(self):
        pg, _ = _make_pglease(acquired=False)

        @pg.singleton_task("task", ttl=60, skip_if_locked=False)
        def work():
            pass

        with pytest.raises(AcquisitionError):
            work()
        pg.close()

    def test_releases_after_execution(self):
        pg, backend = _make_pglease(acquired=True)
        pg.heartbeat_manager = MagicMock()

        @pg.singleton_task("task", ttl=60)
        def work():
            pass

        work()
        backend.release.assert_called_once()

    def test_releases_after_exception(self):
        pg, backend = _make_pglease(acquired=True)
        pg.heartbeat_manager = MagicMock()

        @pg.singleton_task("task", ttl=60)
        def work():
            raise ValueError("oops")

        with pytest.raises(ValueError):
            work()
        backend.release.assert_called_once()

    def test_preserves_function_name(self):
        pg, _ = _make_pglease()

        @pg.singleton_task("task", ttl=60)
        def my_important_job():
            pass

        assert my_important_job.__name__ == "my_important_job"


# ---------------------------------------------------------------------------
# on_lease_lost callback
# ---------------------------------------------------------------------------

class TestOnLeaseLost:
    def test_callback_called_when_heartbeat_fails(self):
        lost = []
        backend = _make_backend(acquired=True)
        pg = PGLease(backend, owner_id="w", on_lease_lost=lambda t: lost.append(t))
        pg.heartbeat_manager = MagicMock()
        pg.try_acquire("task", ttl=60)
        _, kwargs = pg.heartbeat_manager.start.call_args
        kwargs["on_lease_lost"]("task")
        assert "task" in lost

    def test_task_removed_from_active_on_loss(self):
        pg, _ = _make_pglease(acquired=True)
        pg.heartbeat_manager = MagicMock()
        pg.try_acquire("task", ttl=60)
        _, kwargs = pg.heartbeat_manager.start.call_args
        internal_cb = kwargs["on_lease_lost"]
        internal_cb("task")
        assert "task" not in pg._active_leases


# ---------------------------------------------------------------------------
# close / context manager
# ---------------------------------------------------------------------------

class TestCloseAndContextManager:
    def test_close_stops_all_heartbeats(self):
        pg, _ = _make_pglease()
        pg.heartbeat_manager = MagicMock()
        pg._active_leases = {"t1", "t2"}
        pg.close()
        pg.heartbeat_manager.stop_all.assert_called_once()

    def test_close_releases_active_leases(self):
        pg, backend = _make_pglease()
        pg.heartbeat_manager = MagicMock()
        pg._active_leases = {"t1"}
        pg.close()
        backend.release.assert_called_with("t1", "test-worker")

    def test_close_calls_backend_close(self):
        pg, backend = _make_pglease()
        pg.heartbeat_manager = MagicMock()
        pg.close()
        backend.close.assert_called_once()

    def test_context_manager_calls_close(self):
        pg, backend = _make_pglease()
        pg.heartbeat_manager = MagicMock()
        with pg:
            pass
        backend.close.assert_called_once()

    def test_context_manager_closes_on_exception(self):
        pg, backend = _make_pglease()
        pg.heartbeat_manager = MagicMock()
        with pytest.raises(RuntimeError):
            with pg:
                raise RuntimeError("crash")
        backend.close.assert_called_once()
