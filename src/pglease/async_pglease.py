"""Async wrapper around PGLease for asyncio-based applications."""

from __future__ import annotations

import asyncio
import functools
import logging
from concurrent.futures import Executor
from typing import Callable, List, Optional, Union

from .backends.postgres import PostgresBackend
from .exceptions import AcquisitionError
from .models import Lease
from .pglease import PGLease

logger = logging.getLogger(__name__)


class AsyncPGLease:
    """Async wrapper around :class:`PGLease` for asyncio-based services.

    All blocking database calls are dispatched to a thread-pool executor via
    ``asyncio.get_running_loop().run_in_executor()`` so they never block the
    event loop.  The underlying synchronous heartbeat threads continue to
    run in the background as normal.

    Example::

        import asyncio
        from pglease import AsyncPGLease

        async def main():
            async with AsyncPGLease("postgresql://user:pass@localhost/db") as pglease:
                async with pglease.acquire("my-task", ttl=60) as acquired:
                    if acquired:
                        await do_async_work()

        asyncio.run(main())

    .. note::
        The ``on_lease_lost`` callback (if supplied) is invoked on the
        background heartbeat thread — not on the asyncio event loop thread.
        Make the callback thread-safe or schedule work back onto the loop
        with ``loop.call_soon_threadsafe()``.
    """

    def __init__(
        self,
        backend: Union[PGLease, str],
        owner_id: Optional[str] = None,
        heartbeat_interval: int = 10,
        on_lease_lost: Optional[Callable[[str], None]] = None,
        executor: Optional[Executor] = None,
    ):
        """
        Args:
            backend: A :class:`PGLease` instance or a PostgreSQL connection
                string.  When a string is provided a new ``PGLease`` is
                created internally.
            owner_id: Unique identifier for this worker.  Auto-generated if
                not provided.
            heartbeat_interval: Seconds between heartbeats (default: 10).
            on_lease_lost: Callback invoked with the task name when a
                heartbeat thread exits due to failure.  Called on the
                heartbeat thread — keep it thread-safe.
            executor: Optional :class:`~concurrent.futures.Executor` to use
                for blocking calls.  Defaults to the event loop's default
                executor (a ``ThreadPoolExecutor``).
        """
        if isinstance(backend, str):
            self._sync = PGLease(
                backend,
                owner_id=owner_id,
                heartbeat_interval=heartbeat_interval,
                on_lease_lost=on_lease_lost,
            )
        else:
            self._sync = backend
        self._executor = executor

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _run(self, func: Callable, *args, **kwargs):
        """Schedule *func* in the executor and return an awaitable."""
        loop = asyncio.get_running_loop()
        if kwargs:
            func = functools.partial(func, **kwargs)
        return loop.run_in_executor(self._executor, func, *args)

    # ------------------------------------------------------------------
    # Core API
    # ------------------------------------------------------------------

    async def try_acquire(self, task_name: str, ttl: int = 60) -> bool:
        """Non-blocking lease acquisition attempt.  Returns True/False."""
        return await self._run(self._sync.try_acquire, task_name, ttl)

    def acquire(
        self,
        task_name: str,
        ttl: int = 60,
        raise_on_failure: bool = False,
    ) -> "AsyncLeaseContext":
        """Return an async context manager for the lease.

        Example::

            async with pglease.acquire("my-task", ttl=60) as acquired:
                if acquired:
                    await process()

            # Raise on failure:
            async with pglease.acquire("my-task", raise_on_failure=True):
                await process()
        """
        return AsyncLeaseContext(self, task_name, ttl, raise_on_failure)

    async def release(self, task_name: str) -> bool:
        """Release a previously acquired lease."""
        return await self._run(self._sync.release, task_name)

    async def get_lease(self, task_name: str) -> Optional[Lease]:
        """Return the current :class:`Lease` for *task_name*, or ``None``."""
        return await self._run(self._sync.get_lease, task_name)

    async def list_leases(self) -> List[Lease]:
        """Return all leases in the store."""
        return await self._run(self._sync.list_leases)

    async def cleanup_expired(self) -> int:
        """Delete expired lease rows.  Returns the number removed."""
        return await self._run(self._sync.cleanup_expired)

    async def wait_for_lease(
        self,
        task_name: str,
        ttl: int = 60,
        timeout: Optional[float] = 60.0,
        poll_interval: float = 5.0,
    ) -> bool:
        """Block (asynchronously) until the lease is acquired or timeout.

        Uses :func:`asyncio.sleep` between retries so the event loop is
        not blocked between attempts.  Raises :exc:`AcquisitionError` if
        the timeout expires.

        Pass ``None`` or ``float('inf')`` for *timeout* to wait indefinitely.
        Passing ``0`` raises :exc:`ValueError`.
        """
        loop = asyncio.get_running_loop()
        if timeout == 0:
            raise ValueError(
                "timeout=0 is ambiguous; pass None or float('inf') to wait forever."
            )
        if timeout is None or timeout == float("inf"):
            deadline = float("inf")
        else:
            deadline = loop.time() + timeout

        while True:
            if await self.try_acquire(task_name, ttl):
                return True
            remaining = deadline - loop.time()
            if remaining <= 0:
                raise AcquisitionError(
                    f"Timed out waiting {timeout:.1f}s for lease {task_name!r}"
                )
            sleep_for = min(poll_interval, remaining)
            logger.debug(
                f"Lease {task_name!r} not available; retrying in "
                f"{sleep_for:.1f}s ({remaining:.1f}s remaining)"
            )
            await asyncio.sleep(sleep_for)

    async def close(self) -> None:
        """Release all leases and close backend connections."""
        return await self._run(self._sync.close)

    # ------------------------------------------------------------------
    # Async context manager
    # ------------------------------------------------------------------

    async def __aenter__(self) -> "AsyncPGLease":
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.close()

    # ------------------------------------------------------------------
    # Forwarded sync properties
    # ------------------------------------------------------------------

    @property
    def owner_id(self) -> str:
        """The worker identifier used for all leases."""
        return self._sync.owner_id


class AsyncLeaseContext:
    """Async context manager for lease acquisition.

    Returned by :meth:`AsyncPGLease.acquire`.  Releases the lease
    automatically when the ``async with`` block exits.
    """

    def __init__(
        self,
        coordinator: AsyncPGLease,
        task_name: str,
        ttl: int,
        raise_on_failure: bool,
    ):
        self.coordinator = coordinator
        self.task_name = task_name
        self.ttl = ttl
        self.raise_on_failure = raise_on_failure
        self.acquired = False

    async def __aenter__(self) -> bool:
        self.acquired = await self.coordinator.try_acquire(
            self.task_name, self.ttl
        )
        if not self.acquired and self.raise_on_failure:
            raise AcquisitionError(
                f"Failed to acquire lease for {self.task_name}"
            )
        return self.acquired

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        if self.acquired:
            await self.coordinator.release(self.task_name)
