# Quick Start

## Install

```bash
pip install coor
```

## Basic Usage

```python
from pglease import Coordinator

coordinator = Coordinator("postgresql://postgres:password@localhost/postgres")

with coordinator.acquire("my-task", ttl=60) as acquired:
    if acquired:
        print("Running task...")
        do_work()

coordinator.close()
```

Run this script in two terminals simultaneously - only one will execute.

## Decorator

```python
@coordinator.singleton_task("cleanup", ttl=60)
def cleanup():
    print("Only one worker runs this")

cleanup()
```

## Kubernetes

```python
import os

coordinator = Coordinator(
    os.environ["DATABASE_URL"],
    owner_id=os.environ.get("HOSTNAME")  # Pod name
)
```

## Testing

Start PostgreSQL:
```bash
docker run -d -p 5432:5432 -e POSTGRES_PASSWORD=test postgres:14
```

See [examples/](examples/) for more patterns.