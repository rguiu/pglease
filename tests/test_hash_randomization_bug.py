"""
Integration test demonstrating the hash randomization bug in HybridPostgresBackend.

BUG: _task_to_lock_id() uses Python's hash(), which is randomized per process
(PYTHONHASHSEED). This means different workers/pods compute different advisory
lock IDs for the same task name, completely breaking mutual exclusion.

See REPORT.md issue #1.
"""

import os
import subprocess
import sys
import textwrap
import pytest

from pglease.backends.hybrid_postgres import HybridPostgresBackend

POSTGRES_URL = os.environ.get("TEST_POSTGRES_URL")


# ---------------------------------------------------------------------------
# Unit-level: prove hash() is non-deterministic across processes
# ---------------------------------------------------------------------------

class TestHashNonDeterminism:
    """Demonstrate Python hash() randomization without requiring a database."""

    def _get_lock_id_in_subprocess(self, task_name: str) -> int:
        """Spawn a fresh Python process and return the lock ID it computes."""
        code = textwrap.dedent(f"""
            from pglease.backends.hybrid_postgres import HybridPostgresBackend
            print(HybridPostgresBackend._task_to_lock_id({task_name!r}))
        """)
        result = subprocess.run(
            [sys.executable, "-c", code],
            capture_output=True,
            text=True,
            check=True,
        )
        return int(result.stdout.strip())

    def test_same_task_produces_different_lock_ids_across_processes(self):
        """
        Two separate processes compute DIFFERENT lock IDs for the same task name.

        This is the root cause: when deployed as multiple pods, each pod acquires
        its own advisory lock (on a different ID), so they never block each other.
        """
        task_name = "critical-singleton-task"

        ids = {self._get_lock_id_in_subprocess(task_name) for _ in range(5)}

        # With hash randomization, we expect multiple distinct IDs across 5 runs.
        # The probability of all 5 colliding by chance is astronomically small.
        assert len(ids) > 1, (
            f"All 5 processes produced the same lock ID {ids}. "
            "Either PYTHONHASHSEED is fixed in this environment "
            "or the bug has already been fixed."
        )

    def test_lock_ids_vary_per_process(self):
        """Show the actual diverging values for clarity."""
        task_name = "my-task"
        process_ids = [
            self._get_lock_id_in_subprocess(task_name) for _ in range(3)
        ]
        unique_ids = set(process_ids)

        print(f"\nLock IDs computed by 3 separate processes for {task_name!r}:")
        for i, lock_id in enumerate(process_ids, 1):
            print(f"  Process {i}: {lock_id}")

        assert len(unique_ids) > 1, (
            f"Expected different lock IDs across processes, got: {process_ids}"
        )

    def test_within_same_process_hash_is_stable(self):
        """
        Within a single process, hash() is consistent — the bug only manifests
        across process boundaries, exactly as happens between Kubernetes pods.
        """
        task_name = "my-task"
        id1 = HybridPostgresBackend._task_to_lock_id(task_name)
        id2 = HybridPostgresBackend._task_to_lock_id(task_name)
        assert id1 == id2, "hash() should be stable within a single process"


# ---------------------------------------------------------------------------
# Integration: prove two "pods" can hold the lock simultaneously
# ---------------------------------------------------------------------------

@pytest.mark.skipif(not POSTGRES_URL, reason="TEST_POSTGRES_URL not set")
class TestAdvisoryLockBypassIntegration:
    """
    Demonstrate that two simulated workers can both acquire the advisory lock
    for the same task name because they compute different lock IDs.

    Requires a running PostgreSQL instance (TEST_POSTGRES_URL).
    """

    def _acquire_advisory_lock_in_subprocess(self, task_name: str) -> tuple[int, bool]:
        """
        Spawn a subprocess that acquires an advisory lock and reports back
        the lock ID it used and whether acquisition succeeded.
        Returns (lock_id, acquired).
        """
        code = textwrap.dedent(f"""
            import psycopg2
            from psycopg2.extras import RealDictCursor
            from pglease.backends.hybrid_postgres import HybridPostgresBackend

            lock_id = HybridPostgresBackend._task_to_lock_id({task_name!r})
            conn = psycopg2.connect({POSTGRES_URL!r}, cursor_factory=RealDictCursor)
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute("SELECT pg_try_advisory_lock(%s)", (lock_id,))
                acquired = cur.fetchone()[0]
            print(lock_id, acquired)
            # Hold the lock briefly so the parent can check
            import time; time.sleep(1)
        """)
        proc = subprocess.Popen(
            [sys.executable, "-c", code],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        output, _ = proc.communicate()
        lock_id_str, acquired_str = output.strip().split()
        return int(lock_id_str), acquired_str == "True"

    def test_two_workers_both_acquire_same_task_simultaneously(self):
        """
        Two workers (subprocesses simulating separate pods) BOTH successfully
        acquire the advisory lock for the same task name at the same time.

        Expected (buggy) result:   worker1_acquired=True, worker2_acquired=True
        Expected (fixed) result:   worker1_acquired=True, worker2_acquired=False

        This proves the mutual exclusion guarantee is broken.
        """
        task_name = "singleton-job"

        # Run both workers concurrently
        code = textwrap.dedent(f"""
            import psycopg2
            from psycopg2.extras import RealDictCursor
            from pglease.backends.hybrid_postgres import HybridPostgresBackend

            lock_id = HybridPostgresBackend._task_to_lock_id({task_name!r})
            conn = psycopg2.connect({POSTGRES_URL!r}, cursor_factory=RealDictCursor)
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute("SELECT pg_try_advisory_lock(%s)", (lock_id,))
                acquired = cur.fetchone()[0]
            print(lock_id, acquired)
        """)

        proc1 = subprocess.Popen(
            [sys.executable, "-c", code],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        )
        proc2 = subprocess.Popen(
            [sys.executable, "-c", code],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        )

        out1, _ = proc1.communicate()
        out2, _ = proc2.communicate()

        lock_id1, acquired1 = out1.strip().split()
        lock_id2, acquired2 = out2.strip().split()
        lock_id1, lock_id2 = int(lock_id1), int(lock_id2)
        acquired1, acquired2 = acquired1 == "True", acquired2 == "True"

        print(f"\nWorker 1: lock_id={lock_id1}, acquired={acquired1}")
        print(f"Worker 2: lock_id={lock_id2}, acquired={acquired2}")

        if lock_id1 != lock_id2:
            # BUG CONFIRMED: different lock IDs → both acquire simultaneously
            assert acquired1 and acquired2, (
                "Expected both workers to acquire (demonstrating the bug), "
                f"but got acquired1={acquired1}, acquired2={acquired2}"
            )
            pytest.fail(
                f"\n{'='*60}\n"
                f"BUG CONFIRMED: hash() randomization breaks mutual exclusion!\n"
                f"  Task name   : {task_name!r}\n"
                f"  Worker 1 ID : {lock_id1} → acquired={acquired1}\n"
                f"  Worker 2 ID : {lock_id2} → acquired={acquired2}\n"
                f"\n"
                f"Both workers hold the advisory lock simultaneously because\n"
                f"they computed DIFFERENT lock IDs for the same task name.\n"
                f"Fix: replace hash() with hashlib.sha256() in _task_to_lock_id().\n"
                f"{'='*60}"
            )
        else:
            # Hash seeds happened to collide (rare) — at least one should fail
            assert not (acquired1 and acquired2), (
                "Lock IDs matched but both workers acquired — unexpected."
            )
            pytest.skip(
                "PYTHONHASHSEED happened to produce the same ID in both processes. "
                "Re-run the test to observe the bug."
            )
