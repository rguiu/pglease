#!/usr/bin/env python3
"""
Quick demo of pglease API without needing a database.
Shows the three usage patterns and how the library works.
"""

from pglease import Coordinator
from pglease.exceptions import PgleaseError

print("=" * 60)
print("pglease - Distributed Task Coordination Demo")
print("=" * 60)

print("\n📦 Package successfully installed!")
print("   Import: from pglease import Coordinator")

print("\n✅ Three usage patterns available:\n")

print("1. Context Manager (recommended):")
print("""
   with coordinator.acquire("my-task", ttl=60) as acquired:
       if acquired:
           # Do singleton work
           pass
""")

print("2. Decorator:")
print("""
   @coordinator.singleton_task("cleanup", ttl=300)
   def cleanup_job():
       # Automatically coordinated
       pass
""")

print("3. Explicit acquire/release:")
print("""
   if coordinator.try_acquire("sync-job", ttl=120):
       try:
           # Do work
           pass
       finally:
           coordinator.release("sync-job")
""")

print("\n" + "=" * 60)
print("To run examples with real PostgreSQL:")
print("=" * 60)
print("""
# Start PostgreSQL (choose one):
docker run -d -p 5432:5432 -e POSTGRES_PASSWORD=postgres postgres:13

# Or use existing PostgreSQL instance

# Set connection string:
export DATABASE_URL="postgresql://postgres:postgres@localhost:5432/postgres"

# Run examples:
python examples/basic_usage.py
python examples/multi_worker_simulation.py
python examples/hybrid_backend.py
""")

print("\n✨ Library is ready to use!")
