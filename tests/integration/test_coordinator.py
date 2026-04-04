"""Tests for Coordinator."""

import os
import time

import pytest

from pglease import PGLease
from pglease.exceptions import AcquisitionError

POSTGRES_URL = os.environ.get("TEST_POSTGRES_URL")
pytestmark = pytest.mark.skipif(not POSTGRES_URL, reason="TEST_POSTGRES_URL not set")


@pytest.fixture
def coordinator():
    """Create a coordinator for testing."""
    if not POSTGRES_URL:
        pytest.skip("TEST_POSTGRES_URL not set")

    coord = PGLease(POSTGRES_URL, owner_id="test-worker")
    yield coord

    # Clean up
    coord.close()


class TestCoordinator:
    """Test Coordinator functionality."""

    def test_initialization(self):
        """Test coordinator initialization."""
        coord = PGLease(POSTGRES_URL, owner_id="test-worker")
        assert coord.owner_id == "test-worker"
        coord.close()

    def test_auto_owner_id(self):
        """Test automatic owner ID generation."""
        coord = PGLease(POSTGRES_URL)
        assert coord.owner_id is not None
        assert len(coord.owner_id) > 0
        coord.close()

    def test_try_acquire_success(self, coordinator):
        """Test successful lease acquisition."""
        success = coordinator.try_acquire("test-task", ttl=60)
        assert success

        # Clean up
        coordinator.release("test-task")

    def test_try_acquire_locked(self, coordinator):
        """Test acquiring a locked lease."""
        # First acquisition succeeds
        success1 = coordinator.try_acquire("test-task", ttl=60)
        assert success1

        # Second coordinator tries to acquire
        coord2 = PGLease(POSTGRES_URL, owner_id="worker-2")
        success2 = coord2.try_acquire("test-task", ttl=60)
        assert not success2

        # Clean up
        coordinator.release("test-task")
        coord2.close()

    def test_release(self, coordinator):
        """Test lease release."""
        # Acquire and release
        coordinator.try_acquire("test-task", ttl=60)
        released = coordinator.release("test-task")
        assert released

        # Second release should return False
        released2 = coordinator.release("test-task")
        assert not released2

    def test_context_manager_success(self, coordinator):
        """Test context manager with successful acquisition."""
        executed = False

        with coordinator.acquire("test-task", ttl=60) as acquired:
            assert acquired
            executed = True

        assert executed

        # Lease should be released
        lease = coordinator.get_lease("test-task")
        assert lease is None

    def test_context_manager_locked(self, coordinator):
        """Test context manager when lease is locked."""
        # First worker acquires
        coordinator.try_acquire("test-task", ttl=60)

        # Second worker tries with context manager
        coord2 = PGLease(POSTGRES_URL, owner_id="worker-2")
        executed = False

        with coord2.acquire("test-task", ttl=60) as acquired:
            if acquired:
                executed = True

        assert not executed

        # Clean up
        coordinator.release("test-task")
        coord2.close()

    def test_context_manager_wait(self, coordinator):
        """Test context manager with wait=True."""
        # Acquire lease first
        coordinator.try_acquire("test-task", ttl=60)

        # Second worker tries with wait=True
        coord2 = PGLease(POSTGRES_URL, owner_id="worker-2")

        with pytest.raises(AcquisitionError), coord2.acquire("test-task", ttl=60, wait=True):
            pass

        # Clean up
        coordinator.release("test-task")
        coord2.close()

    def test_decorator_success(self, coordinator):
        """Test decorator with successful acquisition."""
        executed = False

        @coordinator.singleton_task("test-task", ttl=60)
        def task():
            nonlocal executed
            executed = True
            return "done"

        result = task()
        assert executed
        assert result == "done"

        # Lease should be released after execution
        lease = coordinator.get_lease("test-task")
        assert lease is None

    def test_decorator_locked(self, coordinator):
        """Test decorator when lease is locked."""
        # Acquire lease first
        coordinator.try_acquire("test-task", ttl=60)

        # Try to run decorated function
        coord2 = PGLease(POSTGRES_URL, owner_id="worker-2")
        executed = False

        @coord2.singleton_task("test-task", ttl=60)
        def task():
            nonlocal executed
            executed = True

        result = task()
        assert not executed
        assert result is None

        # Clean up
        coordinator.release("test-task")
        coord2.close()

    def test_decorator_no_skip(self, coordinator):
        """Test decorator with skip_if_locked=False."""
        # Acquire lease first
        coordinator.try_acquire("test-task", ttl=60)

        # Try to run decorated function
        coord2 = PGLease(POSTGRES_URL, owner_id="worker-2")

        @coord2.singleton_task("test-task", ttl=60, skip_if_locked=False)
        def task():
            pass

        with pytest.raises(AcquisitionError):
            task()

        # Clean up
        coordinator.release("test-task")
        coord2.close()

    def test_heartbeat_keeps_lease_alive(self, coordinator):
        """Test that heartbeat keeps lease alive."""
        # Acquire with short TTL
        coordinator.try_acquire("test-task", ttl=5)

        # Wait longer than TTL
        time.sleep(7)

        # Lease should still be held due to heartbeat
        lease = coordinator.get_lease("test-task")
        assert lease is not None
        assert lease.owner_id == "test-worker"

        # Clean up
        coordinator.release("test-task")

    def test_close_releases_all(self, coordinator):
        """Test that close releases all active leases."""
        # Acquire multiple leases
        coordinator.try_acquire("task-1", ttl=60)
        coordinator.try_acquire("task-2", ttl=60)

        # Close coordinator
        coordinator.close()

        # Leases should be released
        assert coordinator.get_lease("task-1") is None
        assert coordinator.get_lease("task-2") is None
