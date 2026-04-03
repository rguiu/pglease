"""Unit tests for HeartbeatManager — backend is a Mock, no DB required."""

from __future__ import annotations

import threading
import time
from unittest.mock import MagicMock, call, patch

import pytest

from pglease.exceptions import HeartbeatError
from pglease.heartbeat import HeartbeatManager


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_manager(interval: float = 0.05) -> tuple[HeartbeatManager, MagicMock]:
    backend = MagicMock()
    backend.heartbeat.return_value = True
    mgr = HeartbeatManager(backend, interval=interval)
    return mgr, backend


def _wait_for_thread(mgr: HeartbeatManager, task: str, timeout: float = 2.0) -> None:
    """Block until the heartbeat thread for *task* has started."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        with mgr._lock:
            t = mgr._threads.get(task)
        if t and t.is_alive():
            return
        time.sleep(0.005)


# ---------------------------------------------------------------------------
# start / stop basics
# ---------------------------------------------------------------------------

class TestHeartbeatStartStop:
    def test_start_creates_thread(self):
        mgr, _ = _make_manager()
        mgr.start("task-a", "owner-1", ttl=60)
        _wait_for_thread(mgr, "task-a")
        assert "task-a" in mgr._threads
        assert mgr._threads["task-a"].is_alive()
        mgr.stop_all()

    def test_stop_removes_thread(self):
        mgr, _ = _make_manager()
        mgr.start("task-b", "owner", ttl=60)
        _wait_for_thread(mgr, "task-b")
        mgr.stop("task-b")
        assert "task-b" not in mgr._threads

    def test_stop_nonexistent_task_is_noop(self):
        mgr, _ = _make_manager()
        mgr.stop("no-such-task")  # must not raise

    def test_start_replaces_existing_thread(self):
        mgr, backend = _make_manager()
        mgr.start("task-c", "o", ttl=60)
        _wait_for_thread(mgr, "task-c")
        t1 = mgr._threads["task-c"]
        # Start again — should replace
        mgr.start("task-c", "o", ttl=60)
        _wait_for_thread(mgr, "task-c")
        t2 = mgr._threads["task-c"]
        assert t2 is not t1
        mgr.stop_all()

    def test_thread_is_daemon(self):
        mgr, _ = _make_manager()
        mgr.start("task-d", "o", ttl=60)
        _wait_for_thread(mgr, "task-d")
        assert mgr._threads["task-d"].daemon is True
        mgr.stop_all()

    def test_thread_name_contains_task(self):
        mgr, _ = _make_manager()
        mgr.start("my-special-task", "o", ttl=60)
        _wait_for_thread(mgr, "my-special-task")
        name = mgr._threads["my-special-task"].name
        assert "my-special-task" in name
        mgr.stop_all()


# ---------------------------------------------------------------------------
# stop_all
# ---------------------------------------------------------------------------

class TestStopAll:
    def test_stop_all_clears_all_threads(self):
        mgr, _ = _make_manager()
        for i in range(4):
            mgr.start(f"task-{i}", "owner", ttl=60)
        for i in range(4):
            _wait_for_thread(mgr, f"task-{i}")
        mgr.stop_all()
        assert len(mgr._threads) == 0

    def test_stop_all_on_empty_manager_is_noop(self):
        mgr, _ = _make_manager()
        mgr.stop_all()  # must not raise


# ---------------------------------------------------------------------------
# _heartbeat_loop — successful heartbeats
# ---------------------------------------------------------------------------

class TestHeartbeatLoop:
    def test_sends_heartbeat_periodically(self):
        mgr, backend = _make_manager(interval=0.05)
        mgr.start("task", "owner", ttl=30)
        time.sleep(0.25)  # ~4-5 heartbeats
        mgr.stop("task")
        assert backend.heartbeat.call_count >= 2

    def test_heartbeat_called_with_correct_args(self):
        mgr, backend = _make_manager(interval=0.05)
        mgr.start("task-x", "worker-99", ttl=120)
        time.sleep(0.12)
        mgr.stop("task-x")
        backend.heartbeat.assert_called_with("task-x", "worker-99", 120)


# ---------------------------------------------------------------------------
# _heartbeat_loop — failure handling
# ---------------------------------------------------------------------------

class TestHeartbeatLoopFailure:
    def test_heartbeat_returns_false_triggers_on_lease_lost(self):
        """backend.heartbeat() returning False should call on_lease_lost."""
        mgr, backend = _make_manager(interval=0.05)
        backend.heartbeat.return_value = False  # simulate lost lease
        lost = []

        mgr.start("task", "owner", ttl=60, on_lease_lost=lambda t: lost.append(t))
        # Wait for the loop to detect the failure
        deadline = time.monotonic() + 2.0
        while time.monotonic() < deadline and not lost:
            time.sleep(0.01)

        assert "task" in lost

    def test_heartbeat_exception_triggers_on_lease_lost(self):
        """An exception from backend.heartbeat() should also call on_lease_lost."""
        mgr, backend = _make_manager(interval=0.05)
        backend.heartbeat.side_effect = Exception("DB is dead")
        lost = []

        mgr.start("task", "owner", ttl=60, on_lease_lost=lambda t: lost.append(t))
        deadline = time.monotonic() + 2.0
        while time.monotonic() < deadline and not lost:
            time.sleep(0.01)

        assert "task" in lost

    def test_thread_exits_after_failure(self):
        """After a failed heartbeat the background thread should die."""
        mgr, backend = _make_manager(interval=0.05)
        backend.heartbeat.return_value = False
        thread_ref = []

        original_start = mgr.start

        def capturing_start(*args, **kwargs):
            original_start(*args, **kwargs)
            with mgr._lock:
                thread_ref.append(mgr._threads.get(args[0]))

        mgr.start("task", "owner", ttl=60)
        _wait_for_thread(mgr, "task")
        with mgr._lock:
            t = mgr._threads.get("task")
        if t:
            t.join(timeout=2.0)
            assert not t.is_alive()

    def test_on_lease_lost_not_called_on_clean_stop(self):
        """Stopping via stop() must NOT invoke the on_lease_lost callback."""
        mgr, backend = _make_manager(interval=0.5)  # long interval so no heartbeat fires
        lost = []
        mgr.start("task", "owner", ttl=60, on_lease_lost=lambda t: lost.append(t))
        _wait_for_thread(mgr, "task")
        mgr.stop("task")
        assert lost == []

    def test_no_callback_registered_is_safe(self):
        """A failed heartbeat with no on_lease_lost callback must not crash."""
        mgr, backend = _make_manager(interval=0.05)
        backend.heartbeat.return_value = False
        mgr.start("task", "owner", ttl=60)  # no on_lease_lost
        time.sleep(0.3)
        # Should not raise; thread simply exits
