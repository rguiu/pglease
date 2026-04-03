# pglease benchmarks

Measures throughput and latency of core pglease operations against a live PostgreSQL instance.

## Requirements

- A running PostgreSQL server
- pglease installed (`pip install -e .` from the repo root)

## Running

```bash
export TEST_POSTGRES_URL="postgresql://user:pass@localhost/postgres"
python benchmarks/bench_postgres.py
```

Using the Makefile test database:

```bash
make docker-postgres
export TEST_POSTGRES_URL="postgresql://test:test@localhost:5433/coor_test"
python benchmarks/bench_postgres.py
```

## Configuration

All options are set via environment variables:

| Variable | Default | Description |
|---|---|---|
| `TEST_POSTGRES_URL` | *(required)* | PostgreSQL connection string |
| `BENCH_ITERATIONS` | `200` | Iterations per benchmark |
| `BENCH_THREADS` | `8` | Concurrent threads for contention tests |
| `BENCH_POOL_SIZE` | `4` | `pool_size` used in pool comparison benchmarks |
| `BENCH_FILTER` | *(all)* | Only run benchmarks whose name contains this string |

Example — run only pool-related benchmarks with more iterations:

```bash
BENCH_ITERATIONS=500 BENCH_FILTER=pool python benchmarks/bench_postgres.py
```

## What is measured

| Benchmark | What it tests |
|---|---|
| acquire — new lease | INSERT path: task does not exist yet |
| acquire — renew own lease | UPDATE path: same owner re-acquires |
| acquire + release cycle | Full round-trip write throughput |
| heartbeat — active lease | UPDATE heartbeat_at/expires_at |
| get_lease — single row read | SELECT by primary key |
| list_leases — N rows | Full table scan + model hydration |
| cleanup_expired — N rows | DELETE expired rows |
| acquire+release — pool_size=1 vs N | Single connection vs ThreadedConnectionPool |
| acquire+release — standard vs hybrid | PostgresBackend vs HybridPostgresBackend |
| contention — N threads | N threads competing for a single lease slot |

## Interpreting results

- **ops/s** — higher is better; reflects raw throughput
- **ms/op** — lower is better; reflects latency per operation
- The contention benchmark shows how gracefully the library handles concurrent workers racing for the same lease. The "successful acquisitions" note tells you how many threads actually won.
- Pool benchmarks only show a meaningful difference under concurrency; for serial workloads `pool_size=1` is equally fast (and uses fewer DB connections).
