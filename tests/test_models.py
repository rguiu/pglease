"""Tests for core models."""

import pytest
from datetime import datetime, timedelta

from pglease.models import Lease, AcquisitionResult


class TestLease:
    """Test Lease model."""
    
    def test_lease_creation(self):
        """Test creating a lease."""
        now = datetime.utcnow()
        expires = now + timedelta(seconds=60)
        
        lease = Lease(
            task_name="test-task",
            owner_id="worker-1",
            acquired_at=now,
            expires_at=expires,
            heartbeat_at=now,
        )
        
        assert lease.task_name == "test-task"
        assert lease.owner_id == "worker-1"
        assert lease.acquired_at == now
        assert lease.expires_at == expires
    
    def test_is_expired(self):
        """Test lease expiration check."""
        now = datetime.utcnow()
        expired = now - timedelta(seconds=10)
        future = now + timedelta(seconds=60)
        
        lease = Lease(
            task_name="test-task",
            owner_id="worker-1",
            acquired_at=now,
            expires_at=expired,
            heartbeat_at=now,
        )
        
        assert lease.is_expired(now)
        
        lease_active = Lease(
            task_name="test-task",
            owner_id="worker-1",
            acquired_at=now,
            expires_at=future,
            heartbeat_at=now,
        )
        
        assert not lease_active.is_expired(now)
    
    def test_time_remaining(self):
        """Test time remaining calculation."""
        now = datetime.utcnow()
        expires = now + timedelta(seconds=60)
        
        lease = Lease(
            task_name="test-task",
            owner_id="worker-1",
            acquired_at=now,
            expires_at=expires,
            heartbeat_at=now,
        )
        
        remaining = lease.time_remaining(now)
        assert 59.0 <= remaining <= 60.0
        
        # Expired lease should have 0 time remaining
        expired_lease = Lease(
            task_name="test-task",
            owner_id="worker-1",
            acquired_at=now,
            expires_at=now - timedelta(seconds=10),
            heartbeat_at=now,
        )
        
        assert expired_lease.time_remaining(now) == 0.0


class TestAcquisitionResult:
    """Test AcquisitionResult model."""
    
    def test_acquired_result(self):
        """Test successful acquisition result."""
        now = datetime.utcnow()
        lease = Lease(
            task_name="test-task",
            owner_id="worker-1",
            acquired_at=now,
            expires_at=now + timedelta(seconds=60),
            heartbeat_at=now,
        )
        
        result = AcquisitionResult.acquired(lease)
        
        assert result.success
        assert result.lease == lease
        assert result.reason is None
        assert bool(result)  # Should be truthy
    
    def test_failed_result(self):
        """Test failed acquisition result."""
        result = AcquisitionResult.failed("Lease held by another worker")
        
        assert not result.success
        assert result.lease is None
        assert result.reason == "Lease held by another worker"
        assert not bool(result)  # Should be falsy
