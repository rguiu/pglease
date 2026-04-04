# Contributing

## PR Title Convention

This project uses **Conventional Commits** format for **PR titles**. The release
automation reads the PR title (not individual commit messages) to determine the
next version bump.

Commit messages within the branch are free-form, but following the same
convention is encouraged for a clean `git log`.

### PR Title Format
```
<type>[!]: <description>
```

### Types and version bumps:

| PR title prefix | Bump | Example |
|---|---|---|
| `fix:` | Patch (0.1.0 → 0.1.1) | `fix: resolve race condition in release()` |
| `perf:` | Patch (0.1.0 → 0.1.1) | `perf: reduce lock contention in heartbeat` |
| `ci:` | Patch (0.1.0 → 0.1.1) | `ci: switch to setuptools-scm versioning` |
| `feat:` | Minor (0.1.0 → 0.2.0) | `feat: add get_zombie_threads() method` |
| `feat!:` / `fix!:` / `BREAKING CHANGE:` | Major (0.1.0 → 1.0.0) | `feat!: change acquire() return type` |

The `!` must appear **immediately before the colon** (e.g. `feat!:`) to signal a
breaking change. An exclamation mark elsewhere in the title is ignored.

### Workflow:
1. Create a feature branch: `git checkout -b feat/my-feature`
2. Make changes and commit (any message format)
3. Push branch and open a PR with a conventional title
4. Merge the PR to `main` — GitHub Actions automatically:
   - Calculates new version from the PR title
   - Updates version files
   - Creates tag
   - Publishes to Test PyPI
   - Creates GitHub Release draft

See full guide below for details.

---

## Setup

```bash
git clone https://github.com/yourusername/pglease.git
cd pglease
python -m venv venv
source venv/bin/activate
pip install -e ".[dev]"

# Start test database
docker run -d -p 5433:5432 \
  -e POSTGRES_USER=test \
  -e POSTGRES_PASSWORD=test \
  -e POSTGRES_DB=pglease_test \
  postgres:14

export TEST_POSTGRES_URL="postgresql://test:test@localhost:5433/pglease_test"
```

## Running Tests

```bash
pytest
pytest --cov=pglease  # With coverage
```

## Code Style

```bash
black src/ tests/
ruff check src/ tests/
mypy src/
```

## Adding a Backend

Create a new file in `src/pglease/backends/` and implement the `Backend` abstract class. Key requirement: operations must be atomic to prevent race conditions.

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

---

## Conventional Commits Detailed Guide

### All Commit Types

| Type | Version Bump | Description |
|------|--------------|-------------|
| `fix:` | Patch (0.1.1) | Bug fixes |
| `feat:` | Minor (0.2.0) | New features |
| `perf:` | Patch (0.1.1) | Performance improvements |
| `ci:` | Patch (0.1.1) | CI/CD configuration changes |
| `BREAKING CHANGE` | Major (1.0.0) | Breaking API changes |
| `chore:` | None | Maintenance, dependencies |
| `docs:` | None | Documentation only |
| `style:` | None | Formatting (no code change) |
| `refactor:` | None | Code restructure (no feature change) |
| `test:` | None | Adding/fixing tests |

### Detailed Examples

#### Bug Fix (Patch: 0.1.0 → 0.1.1)
```bash
git commit -m "fix: add thread lock to _active_leases set"
git commit -m "fix: prevent zombie threads in heartbeat manager"
git commit -m "fix: resolve race condition in release() method"
```

#### New Feature (Minor: 0.1.0 → 0.2.0)
```bash
git commit -m "feat: add wait_for_lease() method"
git commit -m "feat: add zombie thread monitoring API"
git commit -m "feat: implement retry logic for transient errors"
```

#### Breaking Change (Major: 0.1.0 → 1.0.0)

**Option 1: Using `!`**
```bash
git commit -m "feat!: change acquire() return type to AcquisitionResult"
```

**Option 2: Using footer**
```bash
git commit -m "feat: redesign lease acquisition API

BREAKING CHANGE: acquire() now returns AcquisitionResult instead of bool.
Update code: result = pglease.acquire(...); if result.success: ..."
```

#### Multi-line Commits
```bash
git commit -m "fix: resolve thread safety issues in PGLease

- Add threading.Lock() to protect _active_leases set
- Protect all read-modify-write operations
- Add tests for concurrent access

Closes #42"
```

### Release Workflow

**⚠️ Important: Always work in branches. Never push directly to `main`.**

1. **Create a feature branch**
   ```bash
   git checkout -b fix/race-condition
   # Make your changes
   git add .
   git commit -m "Your detailed commit message"
   git push origin fix/race-condition
   ```

2. **Create Pull Request with conventional title**
   - Go to GitHub and create a PR
   - **Title MUST follow conventional format:**
     - `fix: description` for bug fixes (patch version)
     - `feat: description` for new features (minor version)
     - `feat!: description` for breaking changes (major version)
   - Example: `fix: add thread lock to _active_leases set`

3. **Review and merge PR**
   - Get code review
   - Ensure tests pass
   - Merge the PR (using **merge commit** or **squash and merge**)

4. **Automatic process** (no action needed):
   - ✅ GitHub Actions detects PR title
   - ✅ Calculates new version (based on title)
   - ✅ Commits version bump to `main` with `[skip ci]`
   - ✅ Creates git tag (`v0.1.1`)
   - ✅ Builds package
   - ✅ Publishes to **Test PyPI**
   - ✅ Creates draft GitHub Release

5. **Test on Test PyPI**
   ```bash
   pip install --index-url https://test.pypi.org/simple/ --no-deps pglease==<new_version>
   # Run your tests
   ```

6. **Publish to production (manual)**
   - Go to GitHub → Actions → **"Publish to Production PyPI"**
   - Click **"Run workflow"**
   - **Version tag:** Enter `v0.1.1` (the version you tested)
   - **Confirm:** Type `publish`
   - Click **"Run workflow"**
   - Production PyPI publish happens automatically

### Tips

✅ **Good commits:**
```bash
fix: resolve memory leak in heartbeat threads
feat: add async support via AsyncPGLease  
docs: update README with Kubernetes example
test: add unit tests for PostgresBackend
```

❌ **Bad commits:**
```bash
Fixed bug           # No type prefix
feat added feature  # Wrong format (no colon)
FIX: bug fix       # Uppercase (should be lowercase)
fix bug fix        # No desc after type
```

### Checking What Version Will Be Created

Before pushing:
```bash
# See your last commit
git log -1 --pretty=%B

# Check if it matches conventional format
git log -1 --pretty=%B | grep -E "^(feat|fix|perf):"
```

### Multiple Commits

If you push multiple commits, highest precedence wins:
- `BREAKING CHANGE` > `feat` > `fix` > `perf`

Example:
```bash
git commit -m "fix: bug 1"
git commit -m "fix: bug 2"  
git commit -m "feat: new feature"
git push origin main
# Result: Minor version bump (0.1.0 → 0.2.0)
```

