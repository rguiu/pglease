# pglease

**Distributed task coordination for Python using PostgreSQL.** 

Ensures singleton execution of tasks across multiple workers, pods, or processes. Simple, reliable, and production-ready.

[![PyPI](https://img.shields.io/pypi/v/pglease.svg)](https://pypi.org/project/pglease/)
[![Python](https://img.shields.io/pypi/pyversions/pglease.svg)](https://pypi.org/project/pglease/)
[![License](https://img.shields.io/pypi/l/pglease.svg)](https://github.com/rguiu/pglease/blob/master/LICENSE)

## Why pglease?

- ✅ **Simple API** - Context managers, decorators, or explicit acquire/release
- ✅ **Production-Ready** - Thread-safe, tested, with proper error handling
- ✅ **Observable** - Query who holds what lease and for how long
- ✅ **Async Support** - Full asyncio integration with `AsyncPGLease`
- ✅ **Fast Failover** - Optional hybrid backend for <1s recovery when workers crash
- ✅ **No New Infrastructure** - Uses your existing PostgreSQL database

## The Problem

You have multiple replicas of your application running (Kubernetes pods, containers, or processes), but certain tasks should only run on **one worker at a time**:

- 🔧 Database migrations
- ⏰ Scheduled jobs (cron-like tasks)
- 📊 Report generation
- 🔄 Data synchronization
- ⚡ Leader election
- 🎯 Any critical section needing cluster-wide mutual exclusion

**pglease** solves this by turning PostgreSQL into a distributed lock manager.

## Quick Start

### Installation

```bash
pip install pglease
```

### 30-Second Example

```python
from pglease import PGLease

# Connect to your PostgreSQL database
pglease = PGLease("postgresql://user:pass@localhost/db")

# Only one worker executes this across your entire cluster
with pglease.acquire("daily-report", ttl=300) as acquired:
    if acquired:
        generate_daily_report()  # This runs on exactly one worker
```

That's it! All other workers will skip the task while one holds the lease.

## Adding to an Existing Project

### Step 1: Install pglease

```bash
# Add to your requirements.txt
pip install pglease

# Or with poetry
poetry add pglease

# Or with pipenv
pipenv install pglease
```

### Step 2: Set Up the Database Table

pglease creates the lease table automatically on first use, but you can also create it manually:

```sql
-- Run this migration on your PostgreSQL database
CREATE TABLE IF NOT EXISTS pglease_leases (
    task_name VARCHAR(255) PRIMARY KEY,
    owner_id VARCHAR(255) NOT NULL,
    acquired_at TIMESTAMPTZ NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    heartbeat_at TIMESTAMPTZ NOT NULL
);

-- Optional: Add index for faster lease expiry queries
CREATE INDEX IF NOT EXISTS idx_pglease_expires_at ON pglease_leases(expires_at);
```

**Django:** Create a migration:
```python
# myapp/migrations/0002_create_pglease_table.py
from django.db import migrations

class Migration(migrations.Migration):
    dependencies = [
        ('myapp', '0001_initial'),
    ]
    
    operations = [
        migrations.RunSQL(
            sql="""
                CREATE TABLE IF NOT EXISTS pglease_leases (
                    task_name VARCHAR(255) PRIMARY KEY,
                    owner_id VARCHAR(255) NOT NULL,
                    acquired_at TIMESTAMPTZ NOT NULL,
                    expires_at TIMESTAMPTZ NOT NULL,
                    heartbeat_at TIMESTAMPTZ NOT NULL
                );
            """,
            reverse_sql="DROP TABLE IF EXISTS pglease_leases;"
        ),
    ]
```

**Alembic (Flask/SQLAlchemy):**
```python
# alembic/versions/xxx_add_pglease_table.py
def upgrade():
    op.execute("""
        CREATE TABLE IF NOT EXISTS pglease_leases (
            task_name VARCHAR(255) PRIMARY KEY,
            owner_id VARCHAR(255) NOT NULL,
            acquired_at TIMESTAMPTZ NOT NULL,
            expires_at TIMESTAMPTZ NOT NULL,
            heartbeat_at TIMESTAMPTZ NOT NULL
        );
    """)

def downgrade():
    op.execute("DROP TABLE IF EXISTS pglease_leases;")
```

### Step 3: Initialize pglease in Your Application

**Django Example:**

```python
# myapp/coordination.py
import os
from pglease import PGLease
from django.conf import settings

# Create a singleton instance
_pglease = None

def get_coordinator():
    global _pglease
    if _pglease is None:
        db_url = settings.DATABASES['default']
        connection_string = (
            f"postgresql://{db_url['USER']}:{db_url['PASSWORD']}"
            f"@{db_url['HOST']}:{db_url['PORT']}/{db_url['NAME']}"
        )
        _pglease = PGLease(
            connection_string,
            owner_id=os.environ.get('HOSTNAME', 'django-worker'),
        )
    return _pglease

# Use in your management commands
from django.core.management.base import BaseCommand

class Command(BaseCommand):
    def handle(self, *args, **options):
        coordinator = get_coordinator()
        
        with coordinator.acquire("daily-report", ttl=300) as acquired:
            if acquired:
                self.generate_report()
            else:
                self.stdout.write("Another worker is generating the report")
```

**Flask Example:**

```python
# app/__init__.py
from flask import Flask
from pglease import PGLease
import os

app = Flask(__name__)

# Initialize coordinator
pglease = PGLease(
    os.environ.get('DATABASE_URL'),
    owner_id=os.environ.get('DYNO', 'flask-worker'),  # Heroku
)

# Use in scheduled tasks (e.g., with Flask-APScheduler)
@app.route('/tasks/cleanup')
def cleanup_task():
    with pglease.acquire("cleanup", ttl=300) as acquired:
        if acquired:
            # Do cleanup work
            return {"status": "completed"}
        return {"status": "skipped", "reason": "another worker is running"}

# Cleanup on shutdown
@app.teardown_appcontext
def shutdown_coordinator(exception=None):
    pglease.close()
```

**FastAPI Example:**

```python
# app/main.py
from fastapi import FastAPI
from pglease import AsyncPGLease  # Use async version
import os

app = FastAPI()
pglease = None

@app.on_event("startup")
async def startup():
    global pglease
    pglease = AsyncPGLease(os.environ['DATABASE_URL'])

@app.on_event("shutdown")
async def shutdown():
    await pglease.close()

@app.post("/tasks/sync")
async def sync_task():
    async with pglease.acquire("data-sync", ttl=600) as acquired:
        if acquired:
            await perform_sync()
            return {"status": "completed"}
        return {"status": "skipped"}
```

**Celery Example:**

```python
# tasks.py
from celery import Celery
from pglease import PGLease
import os

app = Celery('tasks')
pglease = PGLease(os.environ['DATABASE_URL'])

@app.task
def process_batch():
    """Ensure only one worker processes batches at a time"""
    with pglease.acquire("batch-processing", ttl=1800) as acquired:
        if acquired:
            # Process the batch
            do_batch_work()
        else:
            # Another worker is processing, safe to skip
            pass
```

### Step 4: Configure Environment Variables

```bash
# .env file
DATABASE_URL=postgresql://user:pass@localhost:5432/mydb

# Kubernetes - use pod name as owner_id
HOSTNAME=my-app-pod-xyz-123

# Heroku - dyno name
DYNO=web.1

# Docker Compose
HOSTNAME=worker-1
```

### Step 5: Use in Your Application

**For Database Migrations:**
```python
# Ensure migration runs only once across all pods
with pglease.acquire("db-migration-v2", ttl=600, raise_on_failure=True):
    apply_migration()
```

**For Scheduled Tasks:**
```python
# Cron job that should run on one worker only
@pglease.singleton_task("hourly-sync", ttl=3600)
def hourly_data_sync():
    sync_external_api()

# Call from all workers - only one executes
hourly_data_sync()
```

**For Background Jobs:**
```python
# Run at app startup, but only on one instance
if pglease.try_acquire("cache-warmup", ttl=300):
    try:
        warm_cache()
    finally:
        pglease.release("cache-warmup")
```

### Step 6: Testing Strategy

**Local Development:**
```python
# Use a separate database or table for testing
pglease = PGLease(
    os.environ.get('TEST_DATABASE_URL', 'postgresql://localhost/test'),
    table_name='pglease_test_leases'  # Separate table
)
```

**Unit Tests:**
```python
import pytest
from pglease import PGLease

@pytest.fixture
def coordinator(postgresql):
    """Provide a test coordinator with clean database"""
    pglease = PGLease(f"postgresql://localhost/{postgresql.info.dbname}")
    yield pglease
    pglease.cleanup_expired()  # Clean up
    pglease.close()

def test_singleton_task(coordinator):
    # First acquisition should succeed
    assert coordinator.try_acquire("test-task", ttl=60) is True
    
    # Second acquisition should fail (lease held)
    assert coordinator.try_acquire("test-task", ttl=60) is False
    
    # After release, should succeed again
    coordinator.release("test-task")
    assert coordinator.try_acquire("test-task", ttl=60) is True
```

### Common Integration Patterns

**Pattern 1: Singleton Service Initialization**
```python
# Run expensive initialization only once
if pglease.try_acquire("ml-model-load", ttl=120):
    try:
        load_ml_model()  # Only one worker loads this
    finally:
        pglease.release("ml-model-load")
```

**Pattern 2: Leader Election**
```python
# One worker becomes the leader
def run_as_leader():
    if pglease.try_acquire("leader", ttl=30):
        try:
            while True:
                do_leader_work()
                time.sleep(10)
                # Heartbeat keeps lease alive
        finally:
            pglease.release("leader")
```

**Pattern 3: Preventing Duplicate Webhooks**
```python
@app.post("/webhook/payment")
def payment_webhook(payload):
    webhook_id = payload['id']
    
    # Prevent duplicate processing if webhook is retried
    with pglease.acquire(f"webhook-{webhook_id}", ttl=300) as acquired:
        if acquired:
            process_payment(payload)
            return {"status": "processed"}
        return {"status": "duplicate", "message": "Already processing"}
```

### Migrating from Other Solutions

**From Redis locks:**
```python
# Before (with redis-py)
import redis
r = redis.Redis()

with r.lock("my-task", timeout=60):
    do_work()

# After (with pglease)
from pglease import PGLease
pglease = PGLease(db_url)

with pglease.acquire("my-task", ttl=60, raise_on_failure=True):
    do_work()
```

**From Celery Beat:**
```python
# Before: Single Celery Beat instance (SPOF)
# After: All workers can try, one executes
@pglease.singleton_task("daily-task", ttl=3600)
def daily_task():
    generate_report()
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

### Using an External Connection Pool or Dependency Injection

If your application already manages a database connection pool (Django ORM, SQLAlchemy, psycopg2 `ThreadedConnectionPool`, or any DI container), you can hand pglease a **connection factory** instead of a connection string.  pglease will borrow a connection for each operation and hand it back immediately afterward — it will never call `connection.close()` and the pool lifecycle is entirely yours.

A **connection factory** is a zero-argument callable that returns a context manager yielding a psycopg2-compatible connection.  pglease calls `commit()` on success and `rollback()` on failure inside the context; your factory's `__exit__` is responsible only for returning the connection to the pool.

#### psycopg2 ThreadedConnectionPool

```python
from contextlib import contextmanager
from psycopg2 import pool
from psycopg2.extras import RealDictCursor
from pglease import PGLease
from pglease.backends.postgres import PostgresBackend

pg_pool = pool.ThreadedConnectionPool(
    minconn=2, maxconn=10,
    dsn="postgresql://user:pass@host/db",
    cursor_factory=RealDictCursor,
)

@contextmanager
def borrow():
    conn = pg_pool.getconn()
    try:
        yield conn
    finally:
        pg_pool.putconn(conn)

backend = PostgresBackend.from_connection_factory(borrow)
pglease = PGLease(backend)
```

#### SQLAlchemy engine

```python
from contextlib import contextmanager
from sqlalchemy import create_engine
from pglease import PGLease
from pglease.backends.postgres import PostgresBackend

engine = create_engine("postgresql+psycopg2://user:pass@host/db")

@contextmanager
def sa_conn():
    conn = engine.raw_connection()
    try:
        yield conn
    finally:
        conn.close()  # returns the connection to SQLAlchemy's pool

backend = PostgresBackend.from_connection_factory(sa_conn)
pglease = PGLease(backend)
```

#### Django (reuse the ORM connection)

```python
from contextlib import contextmanager
from django.db import connection as django_conn
from pglease import PGLease
from pglease.backends.postgres import PostgresBackend

@contextmanager
def django_factory():
    django_conn.ensure_connection()
    yield django_conn.connection
    # pglease commits/rolls back; Django manages the connection lifetime.

backend = PostgresBackend.from_connection_factory(django_factory)
pglease = PGLease(backend)
```

#### FastAPI / dependency injection

```python
from contextlib import contextmanager
import psycopg2
from pglease import PGLease
from pglease.backends.postgres import PostgresBackend

# Shared connection pool, created once at application startup
_pool = psycopg2.pool.ThreadedConnectionPool(2, 10, dsn=DATABASE_URL)

@contextmanager
def get_db_conn():
    conn = _pool.getconn()
    try:
        yield conn
    finally:
        _pool.putconn(conn)

# Single PGLease instance reused across all requests
pglease = PGLease(PostgresBackend.from_connection_factory(get_db_conn))

# ─── FastAPI endpoint ─────────────────────────────────────────────────────────
@app.post("/run-job")
async def run_job():
    with pglease.acquire("my-job", ttl=60) as acquired:
        if acquired:
            do_work()
    return {"ok": True}
```

> **Tip:** When using a connection factory, `close()` on the backend is a no-op — pglease will not close or invalidate any connection from the pool. You are responsible for calling `pg_pool.closeall()` (or the equivalent) at application shutdown.

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

| Use Case | Example | TTL Recommendation |
|----------|---------|--------------------|
| **Database Migrations** | Apply schema changes once | 600s (10 min) |
| **Scheduled Jobs** | Hourly data sync, cleanup tasks | 3600s (1 hour) |
| **Report Generation** | Daily/weekly reports | 1800s (30 min) |
| **Leader Election** | Master selection in cluster | 30s (fast failover) |
| **Batch Processing** | Process queues without duplication | 300s (5 min) |
| **Cache Warming** | Rebuild cache on one instance | 120s (2 min) |

## Features

- **Multiple APIs:** Context managers, decorators, or explicit control
- **Automatic Heartbeats:** Keep leases alive during long-running tasks
- **Graceful Failover:** When a worker crashes, lease expires (TTL) and another takes over
- **Observability:** Query lease table to see who owns what
- **Zombie Detection:** Identify dead workers still holding leases
- **Async Support:** Full asyncio integration
- **Two Backends:** Standard (lease table) or Hybrid (+ advisory locks for <1s failover)
- **Thread-Safe:** All operations properly synchronized
- **Retry Logic:** Resilient to transient database errors

## Production Considerations

### Error Handling

```python
from pglease import PGLease, AcquisitionError

pglease = PGLease(url)

# Option 1: Silent skip if can't acquire
with pglease.acquire("task", ttl=60) as acquired:
    if acquired:
        do_work()
    else:
        logger.info("Another worker is handling this")

# Option 2: Raise exception if can't acquire (fail-fast)
try:
    with pglease.acquire("critical-task", ttl=60, raise_on_failure=True):
        do_critical_work()
except AcquisitionError:
    logger.error("Could not acquire lease - another worker has it")
    sys.exit(1)
```

### Monitoring

```python
# Detect zombie threads (should be zero in healthy system)
zombies = pglease.heartbeat_manager.get_zombie_threads()
if zombies:
    alert(f"Zombie threads detected: {zombies}")

# List active leases
for lease in pglease.list_leases():
    print(f"{lease.task_name}: {lease.owner_id} ({lease.time_remaining():.0f}s left)")

# Clean up expired leases periodically
deleted = pglease.cleanup_expired()
logger.info(f"Cleaned up {deleted} expired leases")
```

### Kubernetes Example

```python
import os
from pglease import PGLease

# Use pod name as owner_id for traceability
pglease = PGLease(
    os.environ["DATABASE_URL"],
    owner_id=os.environ.get("HOSTNAME"),  # e.g., "web-pod-xyz-123"
    heartbeat_interval=10,
    on_lease_lost=lambda task: logger.critical(f"Lost lease: {task}")
)

@pglease.singleton_task("db-migration", ttl=600)
def run_migration():
    """Only runs on one pod even if 10 replicas are deployed"""
    apply_schema_changes()

run_migration()  # Safe to call from all pods
```

## Development & Testing

```bash
# Setup development environment
python -m venv venv
source venv/bin/activate
pip install -e ".[dev]"

# Start test database
docker run -d -p 5432:5432 -e POSTGRES_PASSWORD=test postgres:16

# Run tests
export TEST_POSTGRES_URL="postgresql://postgres:test@localhost/postgres"
pytest                     # Run all tests
pytest --cov=src          # With coverage report
pytest -v                 # Verbose output

# Code quality
ruff check src/ tests/    # Lint
ruff format src/ tests/   # Format
mypy src/                 # Type check

# Build distribution
python -m build

# Install locally for testing
pip install -e .
```

### Running the Demo

```bash
# See examples/ directory for complete working demos
cd examples/
python simple_demo.py
python kubernetes_demo.py
```

## Contributing

Contributions welcome! Please:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Add tests for new functionality
4. Ensure all tests pass (`pytest`)
5. Ensure code quality (`ruff check`, `mypy src/`)
6. Submit a Pull Request

See [CONTRIBUTING.md](CONTRIBUTING.md) for detailed guidelines.

## License

MIT
## Comparison to Alternatives

| Feature | pglease | Redis Lock | ZooKeeper | Celery Beat |
|---------|---------|------------|-----------|-------------|
| **Infrastructure** | PostgreSQL only | Requires Redis | Requires ZK cluster | Requires broker + beat |
| **Failover Time** | TTL-based (10-30s) or <1s (hybrid) | Manual/TTL | Automatic | Single point of failure |
| **Observability** | SQL queries | Limited | Complex API | Web UI |
| **Setup Complexity** | Low | Medium | High | High |
| **Python Async** | ✅ Full support | ✅ | ❌ | ✅ |
| **Thread-Safe** | ✅ | ✅ | ✅ | ⚠️ Beat only |
| **Production Ready** | ✅ | ✅ | ✅ | ⚠️ Single beat instance |

### When to Use pglease

✅ You already use PostgreSQL  
✅ You need simple distributed coordination  
✅ You run multiple replicas/pods/workers  
✅ You want observability via SQL  
✅ You need automatic failover  
✅ You don't want to manage additional infrastructure  

### When NOT to Use pglease

❌ You need sub-100ms lock acquisition  
❌ You're coordinating 1000+ workers on a single task  
❌ You don't have PostgreSQL  
❌ You need distributed transactions (use PostgreSQL 2PC instead)