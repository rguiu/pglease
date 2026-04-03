"""
Tests for thread-safety of PostgresBackend (REPORT.md bug #2).

BUG: PostgresBackend used a single psycopg2 connection with no locking.
The heartbeat thread (one per active lease) called backend.heartbeat()
concurrently with the main thread calling acquire() / release() / get_lease().
psycopg2 connections are explicitly NOT thread-safe, causing:
  - OperationalError: SSL error: decryption failed or bad record mac
  - ProgrammingError: connection is closed
  - Silent data corruption (interleaved protocol messages)

FIX: Every public method acquires self._lock before touching self._conn.

Strategy: instead of a real DB, we inject a fake connection whose cursor
introduces a brief sleep to widen the race window. A ConcurrencyDetector
records how many threads are simultaneously inside the "DB operation".
  - Without the lock → count reaches > 1 (bug reproduced)
  - With    the lock → count stays at 1 (fix confirmed)
"""

import contextlib
import threading
import time
import unittest
from unittest.mock import MagicMock, patch

from pglease.backends.postgres import PostgresBackend


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class ConcurrencyDetector:
    """
    Detects simultaneous access to a critical section from multiple threads.

    Usage:
        detector = ConcurrencyDetector()
        # inside every "DB operation":
        with detector:
            do_db_work()

        assert not detector.violation_detected  # passes if always serialised
    """

    def __init__(self):
        self._counter_lock = threading.Lock()
        self._active = 0
        self.max_concurrent = 0

    @property
    def violation_detected(self) -> bool:
        """True if two or more threads were active simultaneously."""
        return self.max_concurrent > 1

    def __enter__(self):
        with self._counter_lock:
            self._active += 1
            if self._active > self.max_concurrent:
                self.max_concurrent = self._active
        return self

    def __exit__(self, *_):
        with self._counter_lock:
            self._active -= 1


class _NoLock:
    """Drop-in replacement for threading.Lock that does nothing."""
    def __enter__(self): return self
    def __exit__(self, *_): pass


def _make_fake_backend(detector: ConcurrencyDetector, sleep_s: float = 0.01):
    """
    Build a PostgresBackend whose _get_connection() returns a mock that
    spends `sleep_s` seconds inside each cursor execute(), giving other
    threads enough time to enter the same section concurrently.
    """
    def fake_execute(*_args, **_kwargs):
        with detector:
            time.sleep(sleep_s)   # simulate network round-trip

    fake_cursor = MagicMock()
    fake_cursor.__enter__ = lambda s: s
    fake_cursor.__exit__ = MagicMock(return_value=False)
    fake_cursor.execute.side_effect = fake_execute
    fake_cursor.fetchone.return_value = None
    fake_cursor.rowcount = 0

    fake_conn = MagicMock()
    fake_conn.closed = False
    fake_conn.cursor.return_value = fake_cursor

    backend = PostgresBackend.__new__(PostgresBackend)
    backend.connection_string = "postgresql://fake"
    backend.TABLE_NAME = "pglease_leases"
    backend._conn = fake_conn
    backend._lock = threading.Lock()   # real lock (used by the fix)
    backend._get_connection = lambda: fake_conn

    return backend


# ---------------------------------------------------------------------------
# Bug reproduction — without the lock, concurrent access is detectable
# ---------------------------------------------------------------------------

class TestRaceConditionWithoutLock(unittest.TestCase):
    """
    Demonstrate that WITHOUT the lock multiple threads enter the DB critical
    section simultaneously.  This is what happened before bug #2 was fixed.
    """

    THREADS = 8
    CALLS_PER_THREAD = 5

    def _run_concurrent_heartbeats(self, backend):
        """Hammer heartbeat() from many threads at once."""
        errors = []

        def worker():
            for _ in range(self.CALLS_PER_THREAD):
                try:
                    backend.heartbeat("task", "owner", 60)
                except Exception as exc:
                    errors.append(exc)

        threads = [threading.Thread(target=worker) for _ in range(self.THREADS)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        return errors

    def test_concurrent_access_detected_without_lock(self):
        """
        Replacing self._lock with a no-op context manager reproduces the bug:
        the detector observes multiple threads inside the DB operation at once.
        """
        detector = ConcurrencyDetector()
        backend = _make_fake_backend(detector, sleep_s=0.02)

        # Bypass the lock — this is what the code did before the fix
        backend._lock = _NoLock()

        self._run_concurrent_heartbeats(backend)

        self.assertTrue(
            detector.violation_detected,
            f"Expected concurrent access (max_concurrent={detector.max_concurrent}) "
            "but none detected. Increase THREADS or sleep_s.",
        )

    def test_concurrent_access_across_different_methods(self):
        """
        Mix heartbeat(), release(), and get_lease() without the lock;
        at least one pair of calls overlaps.
        """
        detector = ConcurrencyDetector()
        backend = _make_fake_backend(detector, sleep_s=0.02)
        backend._lock = _NoLock()

        errors = []

        def heartbeater():
            for _ in range(5):
                try:
                    backend.heartbeat("task", "owner", 60)
                except Exception as e:
                    errors.append(e)

        def releaser():
            for _ in range(5):
                try:
                    backend.release("task", "owner")
                except Exception as e:
                    errors.append(e)

        def reader():
            for _ in range(5):
                try:
                    backend.get_lease("task")
                except Exception as e:
                    errors.append(e)

        threads = (
            [threading.Thread(target=heartbeater) for _ in range(3)]
            + [threading.Thread(target=releaser) for _ in range(2)]
            + [threading.Thread(target=reader) for _ in range(2)]
        )
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertTrue(
            detector.violation_detected,
            f"Expected concurrent DB access (max={detector.max_concurrent}) "
            "but none detected.",
        )


# ---------------------------------------------------------------------------
# Fix verification — WITH the lock, access is always serialised
# ---------------------------------------------------------------------------

class TestThreadSafetyWithLock(unittest.TestCase):
    """
    Verify that WITH the threading.Lock in place, the DB critical section
    is always entered by exactly one thread at a time.
    """

    THREADS = 8
    CALLS_PER_THREAD = 5

    def test_heartbeat_serialised(self):
        """Concurrent heartbeat() calls never overlap with the real lock."""
        detector = ConcurrencyDetector()
        backend = _make_fake_backend(detector, sleep_s=0.01)
        # backend._lock is a real threading.Lock (set by _make_fake_backend)

        errors = []

        def worker():
            for _ in range(self.CALLS_PER_THREAD):
                try:
                    backend.heartbeat("task", "owner", 60)
                except Exception as exc:
                    errors.append(exc)

        threads = [threading.Thread(target=worker) for _ in range(self.THREADS)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual(errors, [], f"Unexpected errors: {errors}")
        self.assertFalse(
            detector.violation_detected,
            f"Concurrent DB access detected (max={detector.max_concurrent}) "
            "even with the lock — the fix is not working.",
        )

    def test_mixed_methods_serialised(self):
        """
        heartbeat(), release(), get_lease() called from separate threads
        simultaneously — the lock ensures they never overlap.
        """
        detector = ConcurrencyDetector()
        backend = _make_fake_backend(detector, sleep_s=0.01)

        errors = []

        def heartbeater():
            for _ in range(self.CALLS_PER_THREAD):
                try:
                    backend.heartbeat("task", "owner", 60)
                except Exception as e:
                    errors.append(e)

        def releaser():
            for _ in range(self.CALLS_PER_THREAD):
                try:
                    backend.release("task", "owner")
                except Exception as e:
                    errors.append(e)

        def reader():
            for _ in range(self.CALLS_PER_THREAD):
                try:
                    backend.get_lease("task")
                except Exception as e:
                    errors.append(e)

        threads = (
            [threading.Thread(target=heartbeater) for _ in range(3)]
            + [threading.Thread(target=releaser) for _ in range(2)]
            + [threading.Thread(target=reader) for _ in range(2)]
        )
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual(errors, [], f"Unexpected errors: {errors}")
        self.assertFalse(
            detector.violation_detected,
            f"Concurrent DB access detected (max={detector.max_concurrent}) — "
            "lock is not protecting all methods.",
        )

    def test_max_concurrent_is_always_one(self):
        """
        Exhaustive check: across 50 mixed concurrent calls, the detector
        must never observe more than 1 active thread inside the critical section.
        """
        detector = ConcurrencyDetector()
        backend = _make_fake_backend(detector, sleep_s=0.005)

        def spam():
            for _ in range(10):
                backend.heartbeat("t", "o", 30)
                backend.get_lease("t")
                backend.release("t", "o")

        threads = [threading.Thread(target=spam) for _ in range(6)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual(
            detector.max_concurrent, 1,
            f"max_concurrent={detector.max_concurrent}: "
            "DB section entered by multiple threads simultaneously.",
        )


if __name__ == "__main__":
    unittest.main()
