"""
pglease benchmarks — measures throughput and latency of core operations.

Requires a live PostgreSQL instance:
    export TEST_POSTGRES_URL="postgresql://user:pass@localhost/postgres"
    python benchmarks/bench_postgres.py

Options (env vars):
    BENCH_ITERATIONS   Number of iterations per benchmark (default: 200)
    BENCH_THREADS      Number of concurrent threads for contention tests (default: 8)
    BENCH_POOL_SIZE    pool_size for pooled-connection benchmarks (default: 4)
    BENCH_FILTER       Substring filter — only run matching benchmarks
"""

from __future__ import annotations

import os
import sys
import threading
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Callable, List, Optional

# ---------------------------------------------------------------------------
# Configuration from env
# ---------------------------------------------------------------------------

URL = os.environ.get("TEST_POSTGRES_URL")
ITERATIONS = int(os.environ.get("BENCH_ITERATIONS", 200))
THREADS = int(os.environ.get("BENCH_THREADS", 8))
POOL_SIZE = int(os.environ.get("BENCH_POOL_SIZE", 4))
FILTER = os.environ.get("BENCH_FILTER", "").lower()

if not URL:
    print("ERROR: TEST_POSTGRES_URL is not set.")
    print("  export TEST_POSTGRES_URL='postgresql://user:pass@localhost/postgres'")
    sys.exit(1)

# ---------------------------------------------------------------------------
# Lazy imports (requires pglease to be installed / on PYTHONPATH)
# ---------------------------------------------------------------------------

try:
    from pglease.backends.postgres import PostgresBackend
    from pglease.backends.hybrid_postgres import HybridPostgresBackend
    from pglease.pglease import PGLease
except ImportError as exc:
    print(f"ERROR: Could not import pglease: {exc}")
    print("  Make sure pglease is installed: pip install -e .")
    sys.exit(1)


# ---------------------------------------------------------------------------
# Result collection
# ---------------------------------------------------------------------------

@dataclass
class BenchResult:
    name: str
    iterations: int
    elapsed_s: float
    notes: str = ""

    @property
    def ops_per_sec(self) -> float:
        return self.iterations / self.elapsed_s if self.elapsed_s else 0.0

    @property
    def ms_per_op(self) -> float:
        return (self.elapsed_s / self.iterations * 1000) if self.iterations else 0.0


_results: List[BenchResult] = []


def _register(result: BenchResult) -> None:
    _results.append(result)
    # Print inline progress
    print(
        f"  {'OK':>4}  {result.ops_per_sec:>8.1f} ops/s  "
        f"{result.ms_per_op:>7.2f} ms/op  "
        f"[{result.iterations} iters, {result.elapsed_s:.2f}s]"
        + (f"  ({result.notes})" if result.notes else "")
    )


# ---------------------------------------------------------------------------
# Benchmark runner helper
# ---------------------------------------------------------------------------

def bench(
    name: str,
    fn: Callable[[], None],
    iterations: int = ITERATIONS,
    notes: str = "",
    warmup: int = 5,
) -> Optional[BenchResult]:
    """Time *fn* called *iterations* times and record the result."""
    if FILTER and FILTER not in name.lower():
        return None

    print(f"\n{name}")

    # Warmup
    for _ in range(warmup):
        try:
            fn()
        except Exception:
            pass

    start = time.perf_counter()
    for _ in range(iterations):
        fn()
    elapsed = time.perf_counter() - start

    result = BenchResult(name=name, iterations=iterations, elapsed_s=elapsed, notes=notes)
    _register(result)
    return result


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_backend(pool_size: int = 1) -> PostgresBackend:
    return PostgresBackend(URL, pool_size=pool_size)


def _clean(backend: PostgresBackend) -> None:
    """Remove all rows from the lease table."""
    with backend._connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"DELETE FROM {backend.TABLE_NAME}")


def _owner() -> str:
    return f"bench-{uuid.uuid4().hex[:8]}"


# ---------------------------------------------------------------------------
# Benchmarks
# ---------------------------------------------------------------------------

def run_acquire_new(backend: PostgresBackend) -> None:
    """Acquire a fresh (non-existent) lease each time."""
    _clean(backend)

    def fn():
        task = f"task-{uuid.uuid4().hex}"
        backend.acquire(task, _owner(), ttl=60)

    bench("acquire — new lease (no contention)", fn)


def run_acquire_renew(backend: PostgresBackend) -> None:
    """Acquire the same lease repeatedly (renewal path — same owner)."""
    task = f"task-renew-{uuid.uuid4().hex}"
    owner = _owner()
    backend.acquire(task, owner, ttl=60)

    def fn():
        backend.acquire(task, owner, ttl=60)

    bench("acquire — renew own lease", fn)


def run_acquire_release_cycle(backend: PostgresBackend) -> None:
    """Acquire then immediately release — full round-trip."""
    task = f"task-cycle-{uuid.uuid4().hex}"
    owner = _owner()

    def fn():
        backend.acquire(task, owner, ttl=60)
        backend.release(task, owner)

    bench("acquire + release cycle", fn)


def run_heartbeat(backend: PostgresBackend) -> None:
    """Send heartbeats on an active lease."""
    task = f"task-hb-{uuid.uuid4().hex}"
    owner = _owner()
    backend.acquire(task, owner, ttl=3600)

    def fn():
        backend.heartbeat(task, owner, ttl=3600)

    bench("heartbeat — active lease", fn)
    backend.release(task, owner)


def run_get_lease(backend: PostgresBackend) -> None:
    """Read a single lease row."""
    task = f"task-get-{uuid.uuid4().hex}"
    owner = _owner()
    backend.acquire(task, owner, ttl=3600)

    def fn():
        backend.get_lease(task)

    bench("get_lease — single row read", fn)
    backend.release(task, owner)


def run_list_leases(backend: PostgresBackend, n_rows: int = 50) -> None:
    """Scan all leases with N rows present."""
    _clean(backend)
    pre_owner = _owner()
    for i in range(n_rows):
        backend.acquire(f"task-list-{i}", pre_owner, ttl=3600)

    def fn():
        backend.list_leases()

    bench(f"list_leases — {n_rows} rows", fn, notes=f"{n_rows} rows")
    _clean(backend)


def run_cleanup_expired(backend: PostgresBackend, n_rows: int = 50) -> None:
    """cleanup_expired() with N stale rows each run."""
    import psycopg2
    from psycopg2.extras import RealDictCursor
    from datetime import datetime, timezone, timedelta

    def _seed():
        with backend._connection() as conn:
            with conn.cursor() as cur:
                for i in range(n_rows):
                    past = datetime.now(timezone.utc) - timedelta(seconds=10)
                    cur.execute(
                        f"""
                        INSERT INTO {backend.TABLE_NAME}
                            (task_name, owner_id, acquired_at, expires_at, heartbeat_at)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (task_name) DO UPDATE
                          SET expires_at = EXCLUDED.expires_at
                        """,
                        (f"expired-{i}", "bench-seed", past, past, past),
                    )

    def fn():
        _seed()
        backend.cleanup_expired()

    bench(f"cleanup_expired — {n_rows} expired rows", fn, notes=f"{n_rows} rows")


def run_contention(pool_size: int = 1, n_threads: int = THREADS) -> None:
    """N threads all try to hold the same lease — measures contention overhead."""
    backend = _make_backend(pool_size=pool_size)
    task = f"task-contend-{uuid.uuid4().hex}"
    acquired_count = [0]
    lock = threading.Lock()

    barrier = threading.Barrier(n_threads)

    def worker():
        owner = _owner()
        barrier.wait()  # start all threads simultaneously
        for _ in range(ITERATIONS // n_threads):
            if backend.acquire(task, owner, ttl=5).success:
                with lock:
                    acquired_count[0] += 1
                backend.release(task, owner)

    threads = [threading.Thread(target=worker, daemon=True) for _ in range(n_threads)]
    start = time.perf_counter()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    elapsed = time.perf_counter() - start

    total_attempts = ITERATIONS  # approximately
    label = (
        f"contention — {n_threads} threads, pool_size={pool_size}"
    )
    notes = f"{acquired_count[0]} successful acquisitions"
    print(f"\n{label}")
    result = BenchResult(
        name=label,
        iterations=total_attempts,
        elapsed_s=elapsed,
        notes=notes,
    )
    _register(result)
    _clean(backend)
    backend.close()


def run_pool_vs_single() -> None:
    """Compare acquire+release throughput: single connection vs connection pool."""
    for ps in (1, POOL_SIZE):
        backend = _make_backend(pool_size=ps)
        task = f"task-pool-{uuid.uuid4().hex}"
        owner = _owner()

        def fn(b=backend, t=task, o=owner):
            b.acquire(t, o, ttl=60)
            b.release(t, o)

        bench(
            f"acquire+release — pool_size={ps}",
            fn,
            notes=f"pool_size={ps}",
        )
        backend.close()


def run_hybrid_vs_standard() -> None:
    """Compare acquire+release throughput: standard vs hybrid backend."""

    # Standard
    std = _make_backend(pool_size=1)
    task_std = f"task-std-{uuid.uuid4().hex}"
    owner_std = _owner()

    def fn_std():
        std.acquire(task_std, owner_std, ttl=60)
        std.release(task_std, owner_std)

    bench("acquire+release — PostgresBackend (standard)", fn_std)
    std.close()

    # Hybrid
    try:
        hybrid = HybridPostgresBackend(URL)
        task_hyb = f"task-hyb-{uuid.uuid4().hex}"
        owner_hyb = _owner()

        def fn_hyb():
            hybrid.acquire(task_hyb, owner_hyb, ttl=60)
            hybrid.release(task_hyb, owner_hyb)

        bench("acquire+release — HybridPostgresBackend", fn_hyb)
        hybrid.close()
    except Exception as exc:
        print(f"\n  (Hybrid backend skipped: {exc})")


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------

def print_report() -> None:
    if not _results:
        return

    name_w = max(len(r.name) for r in _results) + 2
    sep = "-" * (name_w + 48)

    print(f"\n\n{'=' * (name_w + 48)}")
    print(f"{'BENCHMARK':<{name_w}}  {'ops/s':>10}  {'ms/op':>8}  {'iters':>7}  notes")
    print(sep)
    for r in _results:
        print(
            f"{r.name:<{name_w}}  {r.ops_per_sec:>10.1f}  "
            f"{r.ms_per_op:>8.2f}  {r.iterations:>7}  {r.notes}"
        )
    print(sep)
    print(f"  PostgreSQL: {URL.split('@')[-1]}")
    print(f"  Iterations per bench: {ITERATIONS}  Threads: {THREADS}  Pool size: {POOL_SIZE}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    print(f"pglease benchmarks")
    print(f"  URL        : {URL.split('@')[-1]}")
    print(f"  Iterations : {ITERATIONS}")
    print(f"  Threads    : {THREADS}")
    print(f"  Pool size  : {POOL_SIZE}")
    if FILTER:
        print(f"  Filter     : '{FILTER}'")

    backend = _make_backend(pool_size=1)

    try:
        # Core read/write operations
        run_acquire_new(backend)
        run_acquire_renew(backend)
        run_acquire_release_cycle(backend)
        run_heartbeat(backend)
        run_get_lease(backend)

        # Bulk operations
        run_list_leases(backend, n_rows=10)
        run_list_leases(backend, n_rows=100)
        run_cleanup_expired(backend, n_rows=50)

        # Connection pool comparison
        run_pool_vs_single()

        # Backend comparison
        run_hybrid_vs_standard()

        # Contention (multi-threaded)
        run_contention(pool_size=1, n_threads=THREADS)
        run_contention(pool_size=POOL_SIZE, n_threads=THREADS)

    finally:
        _clean(backend)
        backend.close()

    print_report()


if __name__ == "__main__":
    main()
