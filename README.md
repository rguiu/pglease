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
    acquired_at TIMESTAMP NOT NULL,
    expires_at TIMESTAMP NOT NULL,
    heartbeat_at TIMESTAMP NOT NULL
);
```

You can query this table to see who's holding what:

```sql
SELECT task_name, owner_id, expires_at - NOW() as remaining
FROM pglease_leases WHERE expires_at > NOW();
```

**Trade-off:** If a worker crashes, others wait for TTL to expire (10-30s typically). This is fine for most use cases like migrations or hourly jobs.

### Hybrid Backend (HybridPostgresBackend)

Combines PostgreSQL advisory locks with the lease table. Advisory locks give you instant failover (<1s) when a worker crashes, while the lease table gives you observability.

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
    owner_id="pod-xyz-123",      # Auto-generated if not provided
    heartbeat_interval=10,        # Heartbeat every 10 seconds
)
```

In Kubernetes, use the pod name as owner_id:

```python
import os
pglease = PGLease(
    os.environ["DATABASE_URL"],
    owner_id=os.environ.get("HOSTNAME")
)
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
```

## License

MIT
