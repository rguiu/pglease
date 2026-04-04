"""Integration tests for PGLease — require TEST_POSTGRES_URL.

These extend tests/test_coordinator.py with coverage for:
- wait_for_lease()
- list_leases()
- cleanup_expired()
- on_lease_lost callback
- raise_on_failure context manager option
- PGLease as context manager
"""

from __future__ import annotations

import os
import time
import uuid

import pytest

from pglease import PGLease
from pglease.exceptions import AcquisitionError

POSTGRES_URL = os.environ.get("TEST_POSTGRES_URL")
pytestmark = pytest.mark.skipif(not POSTGRES_URL, reason="TEST_POSTGRES_URL not set")


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _uid() -> str:
    return uuid.uuid4().hex[:8]


@pytest.fixture
def pg():
    coord = PGLease(POSTGRES_URL, owner_id="test-worker-" + _uid(), heartbeat_interval=60)
    yield coord
    coord.close()


# ---------------------------------------------------------------------------
# list_leases
# ---------------------------------------------------------------------------


class TestListLeases:
    def test_empty_cluster_returns_empty(self, pg):
        leases = pg.list_leases()
        assert isinstance(leases, list)

    def test_lists_owned_leases(self, pg):
        task = "list-" + _uid()
        pg.try_acquire(task, ttl=60)
        leases = pg.list_leases()
        names = {lease.task_name for lease in leases}
        assert task in names
        pg.release(task)

    def test_multiple_leases_listed(self, pg):
        tasks = ["list-a-" + _uid(), "list-b-" + _uid(), "list-c-" + _uid()]
        for t in tasks:
            pg.try_acquire(t, ttl=60)
        leases = pg.list_leases()
        names = {lease.task_name for lease in leases}
        for t in tasks:
            assert t in names
        for t in tasks:
            pg.release(t)


# ---------------------------------------------------------------------------
# cleanup_expired
# ---------------------------------------------------------------------------


class TestCleanupExpired:
    def test_does_not_remove_active_leases(self, pg):
        task = "active-" + _uid()
        pg.try_acquire(task, ttl=3600)
        pg.cleanup_expired()
        assert pg.get_lease(task) is not None
        pg.release(task)

    def test_removes_expired_leases(self, pg):
        task = "short-" + _uid()
        pg.try_acquire(task, ttl=1)
        pg.heartbeat_manager.stop(task)  # prevent heartbeat from renewing it
        time.sleep(2)
        removed = pg.cleanup_expired()
        assert removed >= 1

    def test_returns_zero_when_nothing_expired(self, pg):
        task = "long-" + _uid()
        pg.try_acquire(task, ttl=3600)
        count = pg.cleanup_expired()
        assert count == 0
        pg.release(task)


# ---------------------------------------------------------------------------
# wait_for_lease
# ---------------------------------------------------------------------------


class TestWaitForLease:
    def test_acquires_when_immediately_available(self, pg):
        task = "wfl-free-" + _uid()
        result = pg.wait_for_lease(task, ttl=60, timeout=10, poll_interval=0.5)
        assert result is True
        pg.release(task)

    def test_raises_on_timeout_when_blocked(self):
        pg1 = PGLease(POSTGRES_URL, owner_id="holder-" + _uid(), heartbeat_interval=60)
        pg2 = PGLease(POSTGRES_URL, owner_id="waiter-" + _uid(), heartbeat_interval=60)
        task = "wfl-blocked-" + _uid()
        try:
            pg1.try_acquire(task, ttl=60)
            with pytest.raises(AcquisitionError):
                pg2.wait_for_lease(task, ttl=60, timeout=1.5, poll_interval=0.5)
        finally:
            pg1.release(task)
            pg1.close()
            pg2.close()

    def test_raises_value_error_for_timeout_zero(self, pg):
        with pytest.raises(ValueError, match="timeout=0"):
            pg.wait_for_lease("t", ttl=60, timeout=0)

    def test_acquires_after_release_by_other_worker(self):
        pg1 = PGLease(POSTGRES_URL, owner_id="holder-" + _uid(), heartbeat_interval=60)
        pg2 = PGLease(POSTGRES_URL, owner_id="waiter-" + _uid(), heartbeat_interval=60)
        task = "wfl-released-" + _uid()
        try:
            pg1.try_acquire(task, ttl=60)
            # Release in a background thread after a short delay
            import threading

            def release_after_delay():
                time.sleep(0.5)
                pg1.release(task)

            t = threading.Thread(target=release_after_delay)
            t.start()
            result = pg2.wait_for_lease(task, ttl=60, timeout=5, poll_interval=0.3)
            t.join()
            assert result is True
        finally:
            pg2.release(task)
            pg1.close()
            pg2.close()


# ---------------------------------------------------------------------------
# raise_on_failure context manager
# ---------------------------------------------------------------------------


class TestRaiseOnFailure:
    def test_raises_when_lease_held(self, pg):
        task = "rof-" + _uid()
        pg.try_acquire(task, ttl=60)
        pg2 = PGLease(POSTGRES_URL, owner_id="w2-" + _uid(), heartbeat_interval=60)
        try:
            with pytest.raises(AcquisitionError), pg2.acquire(task, ttl=60, raise_on_failure=True):
                pass
        finally:
            pg.release(task)
            pg2.close()

    def test_succeeds_when_free(self, pg):
        task = "rof-free-" + _uid()
        with pg.acquire(task, ttl=60, raise_on_failure=True) as acquired:
            assert acquired is True


# ---------------------------------------------------------------------------
# on_lease_lost callback
# ---------------------------------------------------------------------------


class TestOnLeaseLost:
    def test_callback_called_when_lease_lost(self):
        """After a lease is stolen, next heartbeat triggers on_lease_lost."""
        lost: list[str] = []
        pg = PGLease(
            POSTGRES_URL,
            owner_id="victim-" + _uid(),
            heartbeat_interval=1,  # fast heartbeat for the test
            on_lease_lost=lambda t: lost.append(t),
        )
        pg2 = PGLease(POSTGRES_URL, owner_id="stealer-" + _uid(), heartbeat_interval=60)
        task = "stolen-" + _uid()
        try:
            pg.try_acquire(task, ttl=2)
            time.sleep(3)  # let lease expire
            pg2.try_acquire(task, ttl=60)  # steal it
            # Wait for on_lease_lost to fire (heartbeat should fail at next beat)
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline and not lost:
                time.sleep(0.2)
        finally:
            pg.heartbeat_manager.stop(task)
            pg2.release(task)
            pg.close()
            pg2.close()
        assert task in lost


# ---------------------------------------------------------------------------
# PGLease as a context manager
# ---------------------------------------------------------------------------


class TestPGLeaseContextManager:
    def test_close_called_on_exit(self):
        task = "cm-" + _uid()
        with PGLease(POSTGRES_URL, owner_id="cm-worker-" + _uid()) as pg:
            pg.try_acquire(task, ttl=60)
            assert pg.get_lease(task) is not None
        # After __exit__, leases should be released
        # (verify using fresh connection)
        check = PGLease(POSTGRES_URL, owner_id="check-" + _uid())
        try:
            assert check.get_lease(task) is None
        finally:
            check.close()

    def test_close_called_even_on_exception(self):
        task = "cm-exc-" + _uid()
        with pytest.raises(RuntimeError), PGLease(POSTGRES_URL, owner_id="cm-exc-" + _uid()) as pg:
            pg.try_acquire(task, ttl=60)
            raise RuntimeError("crash")
        check = PGLease(POSTGRES_URL, owner_id="check-exc-" + _uid())
        try:
            assert check.get_lease(task) is None
        finally:
            check.close()
