"""Core data models for pglease."""

from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass(frozen=True)
class Lease:
    """
    Represents a time-bound ownership of a task.
    
    Immutable to ensure thread-safety and clear ownership semantics.
    """
    
    task_name: str
    owner_id: str
    acquired_at: datetime
    expires_at: datetime
    heartbeat_at: datetime
    
    def is_expired(self, now: Optional[datetime] = None) -> bool:
        """Check if the lease has expired."""
        if now is None:
            now = datetime.utcnow()
        return now >= self.expires_at
    
    def time_remaining(self, now: Optional[datetime] = None) -> float:
        """Get remaining time in seconds before lease expires."""
        if now is None:
            now = datetime.utcnow()
        delta = self.expires_at - now
        return max(0.0, delta.total_seconds())
    
    def __repr__(self) -> str:
        return (
            f"Lease(task_name={self.task_name!r}, "
            f"owner_id={self.owner_id!r}, "
            f"expires_at={self.expires_at.isoformat()})"
        )


@dataclass(frozen=True)
class AcquisitionResult:
    """
    Result of a lease acquisition attempt.
    
    Provides clear semantics about whether acquisition succeeded and why.
    """
    
    success: bool
    lease: Optional[Lease] = None
    reason: Optional[str] = None
    
    @classmethod
    def acquired(cls, lease: Lease) -> "AcquisitionResult":
        """Create a successful acquisition result."""
        return cls(success=True, lease=lease, reason=None)
    
    @classmethod
    def failed(cls, reason: str) -> "AcquisitionResult":
        """Create a failed acquisition result."""
        return cls(success=False, lease=None, reason=reason)
    
    def __bool__(self) -> bool:
        """Allow using result in boolean context."""
        return self.success
    
    def __repr__(self) -> str:
        if self.success:
            return f"AcquisitionResult(success=True, lease={self.lease!r})"
        return f"AcquisitionResult(success=False, reason={self.reason!r})"
