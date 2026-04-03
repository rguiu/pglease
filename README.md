# pglease

Distributed task coordination for Python. Ensures singleton execution of tasks across multiple workers or pods.

## The Problem

You have multiple replicas of your application running (Kubernetes pods, for example), but certain tasks should only run on one worker at a time. Database migrations, scheduled jobs, or any critical section that needs cluster-wide mutual exclusion.

## Installation

```bash
pip install pglease
```

## Usage

```python
from pglease import PGLease

pglease = PGLease("postgresql://user:pass@localhost/db")

# Context manager (recommended)
with pglease.acquire("my-task", ttl=60) as acquired:
    if acquired:
        run_migration()

# Raise on failure instead of silently skipping
with pglease.acquire("my-task", ttl=60, raise_on_failure=True):
    run_migration()

# Decorator
@pglease.singleton_task("cleanup", ttl=300)
def cleanup_old_data():
    # Only one worker executes this
    pass

# Explicit control
if pglease.try_acquire("sync-job", ttl=120):
    try:
        sync_data()
    finally:
        pglease.release("sync-job")
```

## How It Works

Uses PostgreSQL for coordination. Each task gets a row in a lease table with an expiry time. Workers try to acquire the lease atomically using `SELECT FOR UPDATE`. Background threads send heartbeats to keep leases alive during long-running tasks.

If a worker crashes, its lease expires (TTL) and another worker can take over.

## Backends

### Standard Backend (PostgresBackend)

Uses a lease table for coordination. Gives you observability and rich metadata.

```sql
CREATE TABLE pglease_leases (
    task_name VARCHAR(255) PRIMARY KEY,
    owner_id VARCHAR(255) NOT NULL,
    acquired_at TIMESTAMPTZ NOT NULL,
    expires_at   TIMESTAMPTZ NOT NULL,
    heartbeat_at TIMESTAMPTZ NOT NULL
);
```

You can query this table to see who's holding what:

```sql
SELECT task_name, owner_id, expires_at - NOW() AS remaining
FROM pglease_leases WHERE expires_at > NOW();
```

**Trade-off:** If a worker crashes, others wait for TTL to expire (10–30 s typically). This is fine for most use cases like migrations or hourly jobs.

### Hybrid Backend (HybridPostgresBackend)

Combines PostgreSQL advisory locks with the lease table. Advisory locks give you instant failover (<1 s) when a worker crashes, while the lease table gives you observability.

```python
from pglease import PGLease, HybridPostgresBackend

backend = HybridPostgresBackend("postgresql://user:pass@localhost/db")
pglease = PGLease(backend)
```

Use this when you need fast recovery and can't wait for TTL expiry.

## Configuration

```python
pglease = PGLease(
    "postgresql://user:pass@localhost/db",
    owner_id="pod-xyz-123",       # Auto-generated if not provided
    heartbeat_interval=10,         # Heartbeat every 10 seconds
    on_lease_lost=handle_lost,     # Called when a heartbeat fails (see below)
)
```

Both `PostgresBackend` and `HybridPostgresBackend` accept additional options:

```python
from pglease.backends.postgres import PostgresBackend

backend = PostgresBackend(
    "postgresql://user:pass@localhost/db",
    connect_timeout=10,   # Seconds before a connection attempt times out
    pool_size=4,          # Max simultaneous DB connections (default: 1)
)
pglease = PGLease(backend)
```

When `pool_size > 1` a `psycopg2.pool.ThreadedConnectionPool` is used so multiple threads can perform DB operations concurrently instead of serialising through a single connection.

In Kubernetes, use the pod name as `owner_id`:

```python
import os
pglease = PGLease(
    os.environ["DATABASE_URL"],
    owner_id=os.environ.get("HOSTNAME"),
)
```

## Waiting for a Lease

`wait_for_lease()` blocks until the lease becomes available or a timeout fires:

```python
# Wait up to 2 minutes, polling every 5 seconds
pglease.wait_for_lease("nightly-report", ttl=300, timeout=120, poll_interval=5)
generate_report()

# Wait indefinitely
pglease.wait_for_lease("nightly-report", ttl=300, timeout=None)
```

Raises `AcquisitionError` on timeout. Passing `timeout=0` raises `ValueError` — use `try_acquire()` for a single non-blocking attempt instead.

## Asyncio Support

`AsyncPGLease` is a drop-in async wrapper that dispatches all blocking DB calls to a thread-pool executor so they never block the event loop:

```python
import asyncio
from pglease import AsyncPGLease

async def main():
    async with AsyncPGLease("postgresql://user:pass@localhost/db") as pglease:
        async with pglease.acquire("my-task", ttl=60) as acquired:
            if acquired:
                await do_async_work()

        # Or wait for the lease asynchronously
        await pglease.wait_for_lease("my-task", ttl=60, timeout=120)
        await do_async_work()

asyncio.run(main())
```

The underlying heartbeat threads continue to run in the background as normal.

## Observability

### List all leases

```python
for lease in pglease.list_leases():
    remaining = lease.time_remaining()
    print(f"{lease.task_name}: {lease.owner_id} — {remaining:.0f}s left")
```

### Detect zombie leases

A zombie lease has not expired yet but its heartbeat has gone silent, indicating a dead worker:

```python
lease = pglease.get_lease("my-task")
if lease and lease.is_zombie(heartbeat_timeout=30):
    # Worker is gone — safe to take over
    pglease.try_acquire("my-task", ttl=60)
```

### Clean up expired rows

Expired rows accumulate when workers crash without calling `release()`. Call this periodically to keep the table tidy:

```python
deleted = pglease.cleanup_expired()
print(f"Removed {deleted} stale lease(s)")
```

### React to heartbeat failure

Supply `on_lease_lost` to be notified (on the heartbeat thread) when a background heartbeat fails:

```python
def handle_lost(task_name: str) -> None:
    logging.critical("Lost lease %s — aborting!", task_name)
    os.abort()

pglease = PGLease(url, on_lease_lost=handle_lost)
```

## Common Use Cases

Database migrations, scheduled jobs that should run once across the cluster, leader election, or any singleton operation in a distributed system.

## Development

```bash
# Setup
python -m venv venv
source venv/bin/activate
pip install -e ".[dev]"

# Start test database
docker run -d -p 5432:5432 -e POSTGRES_PASSWORD=test postgres:14

# Run tests
export TEST_POSTGRES_URL="postgresql://postgres:test@localhost/postgres"
pytest

# Build distribution packages
python -m build
```

## License

MIT
