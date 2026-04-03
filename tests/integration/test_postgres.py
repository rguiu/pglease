"""Tests for PostgreSQL backend."""

import os
import pytest
from datetime import datetime, timedelta

from pglease.backends.postgres import PostgresBackend
from pglease.models import Lease


# Skip tests if no PostgreSQL connection string
POSTGRES_URL = os.environ.get("TEST_POSTGRES_URL")
pytestmark = pytest.mark.skipif(
    not POSTGRES_URL,
    reason="TEST_POSTGRES_URL not set"
)


@pytest.fixture
def backend():
    """Create a PostgreSQL backend for testing."""
    if not POSTGRES_URL:
        pytest.skip("TEST_POSTGRES_URL not set")
    
    backend = PostgresBackend(POSTGRES_URL)
    yield backend
    
    # Clean up
    try:
        conn = backend._get_connection()
        with conn.cursor() as cur:
            cur.execute(f"DELETE FROM {backend.TABLE_NAME}")
        conn.commit()
    finally:
        backend.close()


class TestPostgresBackend:
    """Test PostgreSQL backend."""
    
    def test_initialize(self, backend):
        """Test table initialization."""
        # Should not raise
        backend.initialize()
    
    def test_acquire_new_lease(self, backend):
        """Test acquiring a new lease."""
        result = backend.acquire("test-task", "worker-1", ttl=60)
        
        assert result.success
        assert result.lease is not None
        assert result.lease.task_name == "test-task"
        assert result.lease.owner_id == "worker-1"
    
    def test_acquire_owned_lease(self, backend):
        """Test acquiring a lease we already own."""
        # First acquisition
        result1 = backend.acquire("test-task", "worker-1", ttl=60)
        assert result1.success
        
        # Second acquisition by same owner
        result2 = backend.acquire("test-task", "worker-1", ttl=60)
        assert result2.success
        assert result2.lease.owner_id == "worker-1"
    
    def test_acquire_locked_lease(self, backend):
        """Test acquiring a lease held by another worker."""
        # First worker acquires
        result1 = backend.acquire("test-task", "worker-1", ttl=60)
        assert result1.success
        
        # Second worker tries to acquire
        result2 = backend.acquire("test-task", "worker-2", ttl=60)
        assert not result2.success
        assert "worker-1" in result2.reason
    
    def test_acquire_expired_lease(self, backend):
        """Test acquiring an expired lease."""
        # Acquire with very short TTL
        result1 = backend.acquire("test-task", "worker-1", ttl=1)
        assert result1.success
        
        # Wait for expiry
        import time
        time.sleep(2)
        
        # Different worker can take over
        result2 = backend.acquire("test-task", "worker-2", ttl=60)
        assert result2.success
        assert result2.lease.owner_id == "worker-2"
    
    def test_release(self, backend):
        """Test releasing a lease."""
        # Acquire lease
        result = backend.acquire("test-task", "worker-1", ttl=60)
        assert result.success
        
        # Release lease
        released = backend.release("test-task", "worker-1")
        assert released
        
        # Verify lease is gone
        lease = backend.get_lease("test-task")
        assert lease is None
    
    def test_release_not_owned(self, backend):
        """Test releasing a lease not owned."""
        # Acquire as worker-1
        result = backend.acquire("test-task", "worker-1", ttl=60)
        assert result.success
        
        # Try to release as worker-2
        released = backend.release("test-task", "worker-2")
        assert not released
        
        # Lease should still exist
        lease = backend.get_lease("test-task")
        assert lease is not None
        assert lease.owner_id == "worker-1"
    
    def test_heartbeat(self, backend):
        """Test heartbeat renewal."""
        # Acquire lease
        result = backend.acquire("test-task", "worker-1", ttl=60)
        assert result.success
        original_expires = result.lease.expires_at
        
        # Wait a bit
        import time
        time.sleep(2)
        
        # Send heartbeat with new TTL
        success = backend.heartbeat("test-task", "worker-1", ttl=120)
        assert success
        
        # Verify expiration was extended
        lease = backend.get_lease("test-task")
        assert lease is not None
        assert lease.expires_at > original_expires
    
    def test_heartbeat_not_owned(self, backend):
        """Test heartbeat for lease not owned."""
        # Acquire as worker-1
        result = backend.acquire("test-task", "worker-1", ttl=60)
        assert result.success
        
        # Try heartbeat as worker-2
        success = backend.heartbeat("test-task", "worker-2", ttl=60)
        assert not success
    
    def test_get_lease(self, backend):
        """Test getting lease information."""
        # No lease initially
        lease = backend.get_lease("test-task")
        assert lease is None
        
        # Acquire lease
        result = backend.acquire("test-task", "worker-1", ttl=60)
        assert result.success
        
        # Get lease
        lease = backend.get_lease("test-task")
        assert lease is not None
        assert lease.task_name == "test-task"
        assert lease.owner_id == "worker-1"
