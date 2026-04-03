"""Main PGLease class for distributed task coordination."""

from __future__ import annotations

import functools
import logging
import uuid
from contextlib import contextmanager
from typing import Callable, Optional, TypeVar, Union

from .backend import Backend
from .backends.postgres import PostgresBackend
from .exceptions import AcquisitionError, ReleaseError
from .heartbeat import HeartbeatManager
from .models import AcquisitionResult, Lease

logger = logging.getLogger(__name__)

F = TypeVar("F", bound=Callable)


class PGLease:
    """
    Main API for distributed task coordination.
    
    Provides multiple ways to coordinate task execution:
    - Context manager: `with pglease.acquire(...)`
    - Explicit control: `pglease.try_acquire(...)` + `pglease.release(...)`
    - Decorator: `@pglease.singleton_task(...)`
    """
    
    def __init__(
        self,
        backend: Union[Backend, str],
        owner_id: Optional[str] = None,
        heartbeat_interval: int = 10,
        on_lease_lost: Optional[Callable[[str], None]] = None,
    ):
        """
        Initialize coordinator.

        Args:
            backend: Backend instance or PostgreSQL connection string
            owner_id: Unique identifier for this worker (auto-generated if None)
            heartbeat_interval: Seconds between heartbeats (default: 10)
            on_lease_lost: Optional callback invoked with the task name when
                a background heartbeat thread exits due to failure.  Use this
                to cancel in-flight work, raise an alert, or update metrics.
                The callback runs on the heartbeat thread — keep it short and
                thread-safe.  Example::

                    def handle_lost(task_name):
                        logging.critical("Lost lease: %s — aborting!", task_name)
                        os.abort()

                    pglease = PGLease(url, on_lease_lost=handle_lost)
        """
        # Initialize backend
        if isinstance(backend, str):
            self.backend: Backend = PostgresBackend(backend)
        else:
            self.backend = backend
        
        # Generate owner ID if not provided
        self.owner_id = owner_id or self._generate_owner_id()

        # User-supplied callback for lease-loss events
        self._on_lease_lost = on_lease_lost

        # Initialize heartbeat manager
        self.heartbeat_manager = HeartbeatManager(
            self.backend,
            interval=heartbeat_interval,
        )

        # Track active leases for cleanup
        self._active_leases: set[str] = set()
        
        logger.info(f"Initialized PGLease with owner_id={self.owner_id}")
    
    @staticmethod
    def _generate_owner_id() -> str:
        """Generate a unique owner ID."""
        import socket
        hostname = socket.gethostname()
        unique_id = uuid.uuid4().hex[:8]
        return f"{hostname}-{unique_id}"
    
    def try_acquire(self, task_name: str, ttl: int = 60) -> bool:
        """
        Try to acquire a lease without blocking.
        
        Args:
            task_name: Unique identifier for the task
            ttl: Time-to-live in seconds for the lease
            
        Returns:
            True if lease acquired, False otherwise
            
        Example:
            if coordinator.try_acquire("my-task", ttl=60):
                try:
                    perform_task()
                finally:
                    coordinator.release("my-task")
        """
        result = self.backend.acquire(task_name, self.owner_id, ttl)
        
        if result.success:
            self._active_leases.add(task_name)
            # Start heartbeat to keep lease alive.
            # The internal callback removes the task from _active_leases so
            # that close() does not attempt to release an already-lost lease,
            # then forwards to any user-supplied on_lease_lost handler.
            def _on_lost(lost_task: str) -> None:
                self._active_leases.discard(lost_task)
                if self._on_lease_lost is not None:
                    self._on_lease_lost(lost_task)

            self.heartbeat_manager.start(task_name, self.owner_id, ttl, on_lease_lost=_on_lost)
            logger.info(f"Acquired lease for {task_name}")
            return True
        
        logger.debug(f"Failed to acquire lease for {task_name}: {result.reason}")
        return False
    
    def acquire(self, task_name: str, ttl: int = 60, raise_on_failure: bool = False) -> "LeaseContext":
        """
        Acquire a lease and return a context manager.
        
        Args:
            task_name: Unique identifier for the task
            ttl: Time-to-live in seconds for the lease
            raise_on_failure: If True, raise AcquisitionError when the lease
                cannot be acquired.  If False (default), the context manager
                simply enters with ``acquired=False`` and the body is skipped
                when used with ``if acquired:``.
            
        Returns:
            Context manager for the lease
            
        Example:
            with coordinator.acquire("my-task", ttl=60) as acquired:
                if acquired:
                    perform_task()

            # Raise on failure instead of silently skipping:
            with coordinator.acquire("my-task", raise_on_failure=True):
                perform_task()  # only runs if lease was acquired
        """
        return LeaseContext(self, task_name, ttl, raise_on_failure)
    
    def release(self, task_name: str) -> bool:
        """
        Release a lease.
        
        Args:
            task_name: Unique identifier for the task
            
        Returns:
            True if lease was released, False if not held
        """
        # Stop heartbeat first
        self.heartbeat_manager.stop(task_name)
        
        # Release lease
        released = self.backend.release(task_name, self.owner_id)
        
        if released:
            self._active_leases.discard(task_name)
            logger.info(f"Released lease for {task_name}")
        else:
            logger.debug(f"No lease to release for {task_name}")
        
        return released
    
    def get_lease(self, task_name: str) -> Optional[Lease]:
        """
        Get the current lease for a task.
        
        Args:
            task_name: Unique identifier for the task
            
        Returns:
            Current Lease if exists, None otherwise
        """
        return self.backend.get_lease(task_name)
    
    def singleton_task(
        self,
        task_name: str,
        ttl: int = 60,
        skip_if_locked: bool = True,
    ) -> Callable[[F], F]:
        """
        Decorator for singleton task execution.
        
        Args:
            task_name: Unique identifier for the task
            ttl: Time-to-live in seconds for the lease
            skip_if_locked: If True, skip execution if locked; if False, raise error
            
        Returns:
            Decorated function
            
        Example:
            @coordinator.singleton_task("my-task", ttl=60)
            def my_task():
                perform_critical_operation()
            
            my_task()  # Only runs if lease acquired
        """
        def decorator(func: F) -> F:
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                if self.try_acquire(task_name, ttl):
                    try:
                        return func(*args, **kwargs)
                    finally:
                        self.release(task_name)
                else:
                    if skip_if_locked:
                        logger.info(
                            f"Skipping {func.__name__} - lease held by another worker"
                        )
                        return None
                    else:
                        raise AcquisitionError(
                            f"Failed to acquire lease for {task_name}"
                        )
            
            return wrapper  # type: ignore
        
        return decorator
    
    def close(self) -> None:
        """
        Clean up resources.
        
        Stops all heartbeats, releases all leases, and closes backend.
        """
        logger.info("Closing coordinator")
        
        # Stop all heartbeats
        self.heartbeat_manager.stop_all()
        
        # Release all active leases
        for task_name in list(self._active_leases):
            try:
                self.release(task_name)
            except Exception as e:
                logger.error(f"Error releasing {task_name}: {e}")
        
        # Close backend
        self.backend.close()
    
    def __enter__(self) -> "PGLease":
        """Support using PGLease as a context manager."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Clean up when exiting context."""
        self.close()


class LeaseContext:
    """
    Context manager for lease acquisition.
    
    Automatically releases the lease when exiting the context.
    """
    
    def __init__(
        self,
        coordinator: PGLease,
        task_name: str,
        ttl: int,
        raise_on_failure: bool,
    ):
        self.coordinator = coordinator
        self.task_name = task_name
        self.ttl = ttl
        self.raise_on_failure = raise_on_failure
        self.acquired = False
    
    def __enter__(self) -> bool:
        """Acquire lease when entering context."""
        self.acquired = self.coordinator.try_acquire(self.task_name, self.ttl)
        
        if not self.acquired and self.raise_on_failure:
            raise AcquisitionError(f"Failed to acquire lease for {self.task_name}")
        
        return self.acquired
    
    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Release lease when exiting context."""
        if self.acquired:
            self.coordinator.release(self.task_name)
