# Complete Guide: Building, Testing, and Publishing pglease

## Quick Reference

### 1. Build for Local Demo Use

```bash
cd /Users/raulguiugallardo/Projects/career/pglease
pip install -e .
```

Your demo project will now use the local pglease library. Changes to the source code are reflected immediately (no reinstall needed).

### 2. Release to PyPI Test Account

```bash
cd /Users/raulguiugallardo/Projects/career/pglease

# Install tools
pip install build twine

# Build
rm -rf dist/ build/ src/*.egg-info
python -m build

# Upload to Test PyPI
python -m twine upload --repository testpypi dist/*
# Username: __token__
# Password: <your-test-pypi-token from test.pypi.org>
```

**Get Test PyPI token:** https://test.pypi.org/manage/account/token/

### 3. Release as Proper PyPI Library

```bash
# After testing on test.pypi.org
python -m twine upload dist/*
# Username: __token__
# Password: <your-pypi-token from pypi.org>
```

**Get PyPI token:** https://pypi.org/manage/account/token/

---

## PyPI Package Description

Your package will appear on PyPI as: **https://pypi.org/project/pglease/**

### Short Description (Already in pyproject.toml)
> Distributed task coordination using PostgreSQL - ensures singleton execution across multiple workers/pods

### Long Description (Auto-pulled from README.md)
Your improved README.md will be automatically displayed on PyPI.

### Search Keywords (Users Find You By Searching)
- "distributed lock python"
- "postgresql coordination"
- "kubernetes singleton task"
- "leader election python"
- "distributed synchronization postgresql"
- "cluster coordination python"

These are now in your `pyproject.toml` keywords.

---

## How to Describe It (For PyPI and Documentation)

### Elevator Pitch (30 seconds)
> **pglease** is a distributed task coordination library for Python that ensures singleton execution across multiple workers, pods, or processes. It uses PostgreSQL as the backend - no additional infrastructure needed. Perfect for database migrations, scheduled jobs, or any task that should run on exactly one worker at a time in a distributed system.

### Technical Description
> pglease provides distributed locking and task coordination for Python applications using PostgreSQL. It ensures that critical tasks (migrations, cron jobs, reports) execute on only one worker even when multiple replicas are running. Features automatic heartbeats, graceful failover, full asyncio support, and SQL-based observability. Thread-safe, production-ready, and tested with 75% code coverage.

### Key Features (For Documentation)
- **Simple API:** Context managers, decorators, or explicit acquire/release
- **Automatic Failover:** When a worker crashes, lease expires (TTL) and another takes over
- **Observable:** Query lease table with SQL to see who owns what
- **Async Support:** Full asyncio integration with `AsyncPGLease`
- **Fast Recovery:** Optional hybrid backend for <1s failover using advisory locks
- **No Extra Infrastructure:** Uses your existing PostgreSQL database
- **Production Ready:** Thread-safe, 75% test coverage, proper error handling

### Use Cases
1. **Database Migrations** - Apply schema changes once across all pods
2. **Scheduled Jobs** - Hourly/daily tasks that should run on one worker
3. **Report Generation** - Generate reports without duplication
4. **Leader Election** - Select master in a cluster
5. **Batch Processing** - Process queues without duplicate work
6. **Cache Warming** - Rebuild cache on one instance

### Target Audience
- Python developers building distributed systems
- DevOps teams running Kubernetes/container deployments
- Teams needing simple coordination without ZooKeeper/etcd
- Applications already using PostgreSQL

---

## README.md Improvements Made

✅ **Better Opening Section**
- Added badges (PyPI, Python versions, License)
- Clear "Why pglease?" bullet points
- Added emojis for visual appeal
- Better problem statement

✅ **Quick Start Section**
- 30-second example up front
- Installation instructions
- Immediate value demonstration

✅ **Use Cases Table**
- Clear examples with TTL recommendations
- Helps users understand applicability

✅ **Features Section**
- Comprehensive feature list
- Highlights production-readiness

✅ **Production Considerations**
- Error handling examples
- Monitoring code snippets
- Kubernetes integration example

✅ **Comparison Table**
- Shows pglease vs alternatives (Redis, ZooKeeper, Celery)
- Clear "when to use" and "when not to use" sections

✅ **Better Development Section**
- More commands (coverage, linting, formatting)
- Demo instructions
- Contributing guidelines

---

## Additional Files Created

### 1. QUICKSTART_LOCAL.md
Step-by-step guide for:
- Installing pglease locally for demo
- Building and testing the package
- Publishing workflow
- Version management
- Troubleshooting

### 2. PYPI_DESCRIPTION.md
- PyPI metadata recommendations
- SEO keywords
- Comparison tables
- Badge suggestions
- Social proof template

### 3. PUBLISHING.md (Already existed, fixed)
- Fixed incorrect imports (`Coordinator` → `PGLease`)
- Complete publishing workflow
- GitHub Actions automation
- Troubleshooting guide

---

## Metadata Improvements Made to pyproject.toml

✅ **Better Description**
```toml
description = "Distributed task coordination using PostgreSQL - ensures singleton execution across multiple workers/pods"
```

✅ **More Keywords** (12 total for better search)
```toml
keywords = [
    "distributed", "coordination", "locking", "singleton",
    "kubernetes", "postgresql", "postgres", "task-scheduling",
    "leader-election", "distributed-systems", "cluster", "concurrency"
]
```

✅ **Better Classifiers**
```toml
classifiers = [
    "Development Status :: 4 - Beta",  # Was "3 - Alpha"
    "Topic :: Software Development :: Libraries :: Python Modules",
    "Topic :: System :: Distributed Computing",
    "Framework :: AsyncIO",
    # ... more
]
```

✅ **Updated Python Support**
```toml
requires-python = ">=3.8"  # Was ">=3.7", align with modern Python
```

✅ **Added Email**
```toml
authors = [{name = "Raul Guiu Gallardo", email = "raulguiu@gmail.com"}]
```

---

## Next Steps

### Before Publishing to PyPI

1. **Update Your GitHub URL** in `pyproject.toml`:
   ```toml
   [project.urls]
   Homepage = "https://github.com/your-actual-username/pglease"
   Repository = "https://github.com/your-actual-username/pglease"
   ```

2. **Run Full Test Suite**:
   ```bash
   cd /Users/raulguiugallardo/Projects/career/pglease
   pytest --cov=src
   # Should show 75% coverage, 164 tests passing
   ```

3. **Verify Package Contents**:
   ```bash
   python -m build
   unzip -l dist/pglease-0.1.0-py3-none-any.whl | grep pglease
   ```

4. **Test Installation in Clean Environment**:
   ```bash
   python -m venv /tmp/test_env
   source /tmp/test_env/bin/activate
   pip install dist/pglease-0.1.0-py3-none-any.whl
   python -c "from pglease import PGLease; print('✅ Works')"
   deactivate
   rm -rf /tmp/test_env
   ```

### Publishing Workflow

1. **Test PyPI** (always test first!)
   ```bash
   python -m twine upload --repository testpypi dist/*
   ```

2. **Install from Test PyPI and verify**
   ```bash
   pip install --index-url https://test.pypi.org/simple/ --no-deps pglease
   ```

3. **Production PyPI** (after testing succeeds)
   ```bash
   python -m twine upload dist/*
   ```

4. **Verify on PyPI**
   - Visit: https://pypi.org/project/pglease/
   - Test installation: `pip install pglease`

### After Publishing

1. **Create GitHub Release**
   ```bash
   git tag -a v0.1.0 -m "Initial release"
   git push origin v0.1.0
   ```

2. **Share Your Package**
   - Reddit: r/Python, r/devops, r/kubernetes
   - Hacker News: Show HN: pglease
   - Twitter/LinkedIn
   - Python Weekly newsletter

3. **Monitor Usage**
   - PyPI stats: https://pypistats.org/packages/pglease
   - GitHub stars/forks/issues

---

## Summary

✅ **Local Development:** `pip install -e .` in pglease directory  
✅ **Test Publishing:** `twine upload --repository testpypi dist/*`  
✅ **Production Publishing:** `twine upload dist/*`  
✅ **README:** Improved with better structure, examples, and comparisons  
✅ **Metadata:** Enhanced pyproject.toml for better PyPI discoverability  
✅ **Documentation:** Created QUICKSTART_LOCAL.md and PYPI_DESCRIPTION.md  

Your pglease library is now **ready for PyPI publication**! 🚀

---

## Common Commands Cheatsheet

```bash
# Development
cd /Users/raulguiugallardo/Projects/career/pglease
pip install -e ".[dev]"
pytest --cov=src
ruff check src/ tests/
mypy src/

# Building
rm -rf dist/ build/ src/*.egg-info
python -m build
twine check dist/*

# Publishing
twine upload --repository testpypi dist/*  # Test first
twine upload dist/*                        # Production

# Version Management
# 1. Edit pyproject.toml: version = "0.1.1"
# 2. git tag -a v0.1.1 -m "Release v0.1.1"
# 3. git push origin v0.1.1
# 4. Rebuild and republish
```
