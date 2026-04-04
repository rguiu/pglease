"""
Tests verifying that HybridPostgresBackend._task_to_lock_id() is deterministic
across processes (fix for REPORT.md issue #1).

FIX: _task_to_lock_id() was changed from Python's hash() (randomised per process
via PYTHONHASHSEED) to hashlib.sha256(), which is fully deterministic.

See REPORT.md issue #1.
"""

import hashlib
import os
import subprocess
import sys
import textwrap

import pytest

from pglease.backends.hybrid_postgres import HybridPostgresBackend

POSTGRES_URL = os.environ.get("TEST_POSTGRES_URL")


# ---------------------------------------------------------------------------
# Unit-level: prove _task_to_lock_id is deterministic across processes
# ---------------------------------------------------------------------------


class TestHashDeterminism:
    """Verify lock IDs are identical across processes (no hash() randomisation)."""

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

    def test_same_task_produces_identical_lock_ids_across_processes(self):
        """
        Multiple separate processes must compute the SAME lock ID for the same
        task name so that advisory locks provide real mutual exclusion between pods.
        """
        task_name = "critical-singleton-task"

        ids = {self._get_lock_id_in_subprocess(task_name) for _ in range(5)}

        assert len(ids) == 1, (
            f"Processes produced different lock IDs for {task_name!r}: {ids}. "
            "hash() randomisation is still in use — fix not applied."
        )

    def test_lock_id_matches_expected_sha256_value(self):
        """Lock ID must equal the first 8 bytes of SHA-256, masked to 63 bits."""
        task_name = "my-task"
        digest = hashlib.sha256(task_name.encode()).digest()
        expected = int.from_bytes(digest[:8], "big") & ((2**63) - 1)

        actual = HybridPostgresBackend._task_to_lock_id(task_name)

        assert actual == expected, (
            f"Lock ID {actual} does not match expected SHA-256-derived value {expected}."
        )

    def test_lock_id_is_stable_within_process(self):
        """Multiple calls within the same process must return the same value."""
        task_name = "my-task"
        id1 = HybridPostgresBackend._task_to_lock_id(task_name)
        id2 = HybridPostgresBackend._task_to_lock_id(task_name)
        assert id1 == id2

    def test_lock_id_is_within_postgres_bigint_range(self):
        """Result must fit in a signed 64-bit integer (PostgreSQL bigint)."""
        lock_id = HybridPostgresBackend._task_to_lock_id("any-task")
        assert 0 <= lock_id < 2**63


# ---------------------------------------------------------------------------
# Integration: prove two "pods" cannot hold the lock simultaneously (fix)
# ---------------------------------------------------------------------------


@pytest.mark.skipif(not POSTGRES_URL, reason="TEST_POSTGRES_URL not set")
class TestAdvisoryLockMutualExclusionIntegration:
    """
    Verify that two simulated workers cannot both hold the same advisory lock
    because they now compute identical deterministic lock IDs.

    Requires a running PostgreSQL instance (TEST_POSTGRES_URL).
    """

    def test_second_worker_cannot_acquire_while_first_holds_lock(self):
        """
        With the SHA-256 fix in place, both workers compute the same lock ID,
        so PostgreSQL's advisory lock correctly blocks the second acquisition.

        Expected result:  worker1_acquired=True, worker2_acquired=False
        """
        task_name = "singleton-job"

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
            import time; time.sleep(2)  # hold the lock while the other worker tries
        """)

        proc1 = subprocess.Popen(
            [sys.executable, "-c", code],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

        import time

        time.sleep(0.3)  # let proc1 acquire before proc2 starts

        code_no_hold = textwrap.dedent(f"""
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

        proc2 = subprocess.Popen(
            [sys.executable, "-c", code_no_hold],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

        out1, err1 = proc1.communicate()
        out2, err2 = proc2.communicate()

        lock_id1, acquired1 = out1.strip().split()
        lock_id2, acquired2 = out2.strip().split()
        lock_id1, lock_id2 = int(lock_id1), int(lock_id2)
        acquired1, acquired2 = acquired1 == "True", acquired2 == "True"

        print(f"\nWorker 1: lock_id={lock_id1}, acquired={acquired1}")
        print(f"Worker 2: lock_id={lock_id2}, acquired={acquired2}")

        assert lock_id1 == lock_id2, (
            f"Lock IDs differ ({lock_id1} vs {lock_id2}) — hash() randomisation "
            "is still in use. Fix not applied."
        )
        assert acquired1, "Worker 1 should have acquired the lock"
        assert not acquired2, (
            "Worker 2 should NOT have acquired the lock while Worker 1 holds it. "
            "Mutual exclusion is broken."
        )
