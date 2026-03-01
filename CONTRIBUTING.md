# Contributing

## Setup

```bash
git clone https://github.com/yourusername/coor.git
cd coor
python -m venv venv
source venv/bin/activate
pip install -e ".[dev]"

# Start test database
docker run -d -p 5433:5432 \
  -e POSTGRES_USER=test \
  -e POSTGRES_PASSWORD=test \
  -e POSTGRES_DB=coor_test \
  postgres:14

export TEST_POSTGRES_URL="postgresql://test:test@localhost:5433/coor_test"
```

## Running Tests

```bash
pytest
pytest --cov=coor  # With coverage
```

## Code Style

```bash
black src/ tests/
ruff check src/ tests/
mypy src/
```

## Adding a Backend

Create a new file in `src/coor/backends/` and implement the `Backend` abstract class. Key requirement: operations must be atomic to prevent race conditions.

```python
from pglease.backend import Backend

class RedisBackend(Backend):
    def acquire(self, task_name, owner_id, ttl):
        # Use SET NX EX for atomicity
        pass
```

Add tests in `tests/` and update docs.

## Pull Requests

Fork, branch, code, test, commit, push, PR. Keep changes focused and add tests.
