"""Heartbeat management for keeping leases alive."""

import logging
import threading
import time
from typing import Callable, Dict, List, Optional

from .backend import Backend
from .exceptions import HeartbeatError

logger = logging.getLogger(__name__)


class HeartbeatManager:
    """
    Manages background heartbeat threads for active leases.

    Each task gets its own heartbeat thread that periodically renews
    the lease until stopped.

    When a heartbeat fails (DB connection lost, lease stolen by another
    worker), the optional ``on_lease_lost`` callback passed to
    :meth:`start` is invoked with the task name so that the caller can
    react immediately rather than discovering the loss after the fact.
    """

    def __init__(self, backend: Backend, interval: int = 10):
        """
        Initialize heartbeat manager.

        Args:
            backend: Backend to send heartbeats through
            interval: Seconds between heartbeats (default: 10)
        """
        self.backend = backend
        self.interval = interval
        self._threads: Dict[str, threading.Thread] = {}
        self._stop_events: Dict[str, threading.Event] = {}
        self._callbacks: Dict[str, Optional[Callable[[str], None]]] = {}
        self._lock = threading.RLock()  # RLock: re-entrant so start() can call stop() while holding the lock
        # HIGH-002: threads that did not exit within the join timeout are
        # tracked here so callers can observe the leak and alert on it.
        self._zombie_threads: Dict[str, threading.Thread] = {}

    def start(
        self,
        task_name: str,
        owner_id: str,
        ttl: int,
        on_lease_lost: Optional[Callable[[str], None]] = None,
    ) -> None:
        """
        Start heartbeat thread for a task.

        Args:
            task_name: Task to send heartbeats for
            owner_id: Owner of the lease
            ttl: TTL to renew with each heartbeat
            on_lease_lost: Optional callback invoked with ``task_name``
                when the heartbeat thread exits due to a failure (i.e.
                *not* when it is stopped cleanly via :meth:`stop`).
                Called from the background thread — keep it short and
                thread-safe.
        """
        with self._lock:
            # Stop existing heartbeat if running
            if task_name in self._threads:
                self.stop(task_name)

            # Create stop event
            stop_event = threading.Event()
            self._stop_events[task_name] = stop_event
            self._callbacks[task_name] = on_lease_lost

            # Create and start thread
            thread = threading.Thread(
                target=self._heartbeat_loop,
                args=(task_name, owner_id, ttl, stop_event),
                daemon=True,
                name=f"pglease-heartbeat-{task_name}",
            )
            self._threads[task_name] = thread
            thread.start()

            logger.debug(f"Started heartbeat for {task_name}")
    
    def stop(self, task_name: str) -> None:
        """
        Stop heartbeat thread for a task.

        Args:
            task_name: Task to stop heartbeat for
        """
        # Signal the thread to stop, but do NOT hold self._lock while
        # joining.  If the heartbeat thread's on_lease_lost callback calls
        # pglease.close() or pglease.release(), those call stop() again,
        # which would try to acquire self._lock — deadlock.
        with self._lock:
            if task_name not in self._threads:
                return

            # Signal thread to stop
            stop_event = self._stop_events.get(task_name)
            if stop_event:
                stop_event.set()

            thread = self._threads.get(task_name)

        # Join outside the lock so on_lease_lost callbacks are free to
        # call back into the coordinator without deadlocking.
        if thread and thread.is_alive():
            thread.join(timeout=30.0)
            if thread.is_alive():
                # HIGH-002: thread did not exit — it is probably blocked on a
                # database operation.  We cannot kill OS threads in Python, so
                # we demote it to "zombie" status: it is no longer tracked as
                # an active heartbeat but is recorded so operators can observe
                # the leak and investigate the root cause.
                logger.error(
                    f"Heartbeat thread for {task_name} did not exit within "
                    "30 s. Marking as zombie — database may be unresponsive. "
                    "Call get_zombie_threads() to list all zombie tasks."
                )
                with self._lock:
                    self._zombie_threads[task_name] = thread

        # Clean up after the thread has finished (or timed out).
        with self._lock:
            self._threads.pop(task_name, None)
            self._stop_events.pop(task_name, None)
            self._callbacks.pop(task_name, None)

            logger.debug(f"Stopped heartbeat for {task_name}")
    
    def stop_all(self) -> None:
        """Stop all heartbeat threads."""
        with self._lock:
            task_names = list(self._threads.keys())

        for task_name in task_names:
            self.stop(task_name)

    def get_zombie_threads(self) -> List[str]:
        """Return task names whose heartbeat threads became zombies.

        A zombie thread failed to exit within the 30-second join timeout
        in :meth:`stop`, typically because it is blocked on a database
        operation.  This method is intended for monitoring and alerting.

        Returns:
            List of task names with zombie heartbeat threads.
        """
        with self._lock:
            return list(self._zombie_threads.keys())
    
    def _heartbeat_loop(
        self,
        task_name: str,
        owner_id: str,
        ttl: int,
        stop_event: threading.Event,
    ) -> None:
        """
        Background loop that sends periodic heartbeats.

        Runs until stop_event is set or heartbeat fails.
        When it exits due to a *failure* (not a clean stop), the
        ``on_lease_lost`` callback registered for this task is invoked.
        """
        logger.info(f"Heartbeat loop started for {task_name} (interval={self.interval}s)")
        failed = False
        _max_retries = 3

        while not stop_event.is_set():
            # Wait for interval or until stopped
            if stop_event.wait(timeout=self.interval):
                break

            # MEDIUM-001: retry transient (non-lease-loss) errors with
            # exponential backoff before declaring the heartbeat as failed.
            for attempt in range(_max_retries):
                try:
                    success = self.backend.heartbeat(task_name, owner_id, ttl)

                    if not success:
                        # Lease is genuinely gone — no point retrying.
                        raise HeartbeatError(
                            f"Heartbeat failed for {task_name}: lease no longer "
                            f"owned by {owner_id} (stolen or expired)"
                        )
                    break  # heartbeat succeeded

                except HeartbeatError as e:
                    logger.error(str(e))
                    failed = True
                    break  # do not retry — lease is lost

                except Exception as e:
                    if attempt < _max_retries - 1:
                        backoff = 2 ** attempt  # 1 s, 2 s
                        logger.warning(
                            f"Transient heartbeat error for {task_name} "
                            f"(attempt {attempt + 1}/{_max_retries}): {e}. "
                            f"Retrying in {backoff}s…"
                        )
                        time.sleep(backoff)
                    else:
                        logger.error(
                            f"Heartbeat for {task_name} failed after "
                            f"{_max_retries} attempts: {e}"
                        )
                        failed = True

            if failed:
                break

        logger.info(f"Heartbeat loop stopped for {task_name}")

        if failed:
            callback = self._callbacks.get(task_name)
            if callback is not None:
                try:
                    callback(task_name)
                except Exception as cb_exc:
                    logger.error(
                        f"on_lease_lost callback raised for {task_name}: {cb_exc}"
                    )
