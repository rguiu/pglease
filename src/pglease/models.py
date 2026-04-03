"""Core data models for pglease."""

from dataclasses import dataclass
from datetime import datetime, timezone
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
            now = datetime.now(timezone.utc)
        return now >= self.expires_at
    
    def time_remaining(self, now: Optional[datetime] = None) -> float:
        """Get remaining time in seconds before lease expires."""
        if now is None:
            now = datetime.now(timezone.utc)
        delta = self.expires_at - now
        return max(0.0, delta.total_seconds())

    def is_zombie(
        self,
        heartbeat_timeout: float = 60.0,
        now: Optional[datetime] = None,
    ) -> bool:
        """Check whether this lease is a zombie.

        A zombie is a lease that has not expired (``expires_at`` is in the
        future) but whose heartbeat has been silent for longer than
        ``heartbeat_timeout`` seconds.  This indicates the worker's heartbeat
        thread died while the lease table row was not yet naturally expired —
        i.e. the worker is no longer actively holding the lease even though
        the row says otherwise.

        Args:
            heartbeat_timeout: Seconds of heartbeat silence that qualify as
                zombie.  Should typically be set to a small multiple of the
                ``heartbeat_interval`` used when creating ``PGLease``
                (default: 60 s).
            now: Reference time.  Defaults to ``datetime.now(timezone.utc)``.

        Returns:
            ``True`` if the lease is active-looking but heartbeat-silent.

        Example::

            lease = pglease.get_lease("my-task")
            if lease and lease.is_zombie(heartbeat_timeout=30):
                # safe to take over — worker has gone quiet
                pglease.try_acquire("my-task", ttl=60)
        """
        if now is None:
            now = datetime.now(timezone.utc)
        if self.is_expired(now):
            return False  # expired naturally — not a zombie
        heartbeat_age = (now - self.heartbeat_at).total_seconds()
        return heartbeat_age > heartbeat_timeout
    
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
