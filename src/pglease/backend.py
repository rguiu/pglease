"""Abstract backend interface for pglease."""

from abc import ABC, abstractmethod
from datetime import datetime
from typing import List, Optional

from .models import Lease, AcquisitionResult


class Backend(ABC):
    """
    Abstract backend for distributed coordination.
    
    Backends must provide atomic lease operations with proper guarantees:
    - Acquisition must be atomic (no race conditions)
    - Only one owner can hold a lease at a time
    - Expired leases can be taken over automatically
    - Heartbeats extend lease lifetime
    """
    
    @abstractmethod
    def acquire(self, task_name: str, owner_id: str, ttl: int) -> AcquisitionResult:
        """
        Attempt to acquire a lease for the given task.
        
        Args:
            task_name: Unique identifier for the task
            owner_id: Unique identifier for the requesting worker
            ttl: Time-to-live in seconds for the lease
            
        Returns:
            AcquisitionResult indicating success/failure
            
        This operation must be atomic and guarantee:
        - Only one owner can acquire the lease
        - Expired leases are automatically available
        - No race conditions between workers
        """
        pass
    
    @abstractmethod
    def release(self, task_name: str, owner_id: str) -> bool:
        """
        Release a lease if owned by the given worker.
        
        Args:
            task_name: Unique identifier for the task
            owner_id: Unique identifier for the worker releasing the lease
            
        Returns:
            True if lease was released, False if not owned by this worker
            
        This operation must be idempotent and only release leases
        owned by the specified owner_id.
        """
        pass
    
    @abstractmethod
    def heartbeat(self, task_name: str, owner_id: str, ttl: int) -> bool:
        """
        Renew a lease by updating its expiration time.
        
        Args:
            task_name: Unique identifier for the task
            owner_id: Unique identifier for the worker holding the lease
            ttl: Time-to-live in seconds to extend the lease
            
        Returns:
            True if heartbeat succeeded, False if lease not owned by this worker
            
        This operation must be atomic and only update leases owned by
        the specified owner_id.
        """
        pass
    
    @abstractmethod
    def get_lease(self, task_name: str) -> Optional[Lease]:
        """
        Get the current lease for a task.
        
        Args:
            task_name: Unique identifier for the task
            
        Returns:
            Current Lease if exists (even if expired), None otherwise
        """
        pass
    
    @abstractmethod
    def initialize(self) -> None:
        """
        Initialize backend (create tables, indexes, etc.).
        
        Must be idempotent - safe to call multiple times.
        """
        pass
    
    @abstractmethod
    def close(self) -> None:
        """
        Clean up backend resources (close connections, etc.).
        
        Must be idempotent - safe to call multiple times.
        """
        pass

    @abstractmethod
    def cleanup_expired(self) -> int:
        """
        Delete all expired lease rows from the store.

        Returns the number of rows removed.  Safe to call at any time —
        active (non-expired) leases are never touched.  Recommended to run
        periodically (e.g. every hour or via a cron job) to prevent the
        lease table from growing without bound.
        """
        pass

    @abstractmethod
    def list_leases(self) -> List[Lease]:
        """
        Return all current leases in the store (active and expired).

        Useful for monitoring dashboards, health checks, and debugging.
        Returns an empty list if no leases exist.
        """
        pass
