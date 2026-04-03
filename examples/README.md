# Examples

Start PostgreSQL:
```bash
docker run -d -p 5432:5432 -e POSTGRES_PASSWORD=password postgres:14
```

## Files

- **basic_usage.py** - Context manager, decorator, explicit control
- **kubernetes_app.py** - K8s deployment with migrations and scheduled tasks
- **multi_worker_simulation.py** - Testing concurrent workers
- **scheduled_tasks.py** - APScheduler integration
- **hybrid_backend.py** - Fast failover with hybrid backend

## Common Patterns

Startup migration:
```python
@pglease.singleton_task("migration", ttl=300)
def migrate():
    apply_migrations()
```

Scheduled job:
```python
@scheduler.scheduled_job('cron', hour=0)
@pglease.singleton_task("daily", ttl=3600)
def daily_job():
    generate_reports()
```

Queue processing:
```python
for item in queue:
    if pglease.try_acquire(f"item-{item.id}", ttl=300):
        try:
            process(item)
        finally:
            pglease.release(f"item-{item.id}")
```
