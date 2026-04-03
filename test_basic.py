"""Test basic usage with real PostgreSQL."""

import time
from pglease import Coordinator

# Use the Docker PostgreSQL we just started
coordinator = Coordinator("postgresql://postgres:postgres@localhost:5432/postgres")

print("=" * 60)
print("Testing pglease with PostgreSQL")
print("=" * 60)

# Test 1: Context Manager
print("\n1. Testing Context Manager pattern...")
with coordinator.acquire("daily-report", ttl=60) as acquired:
    if acquired:
        print("   ✓ Acquired lease for 'daily-report'")
        print("   → Doing work...")
        time.sleep(1)
        print("   ✓ Work completed")
    else:
        print("   ✗ Could not acquire lease (another worker has it)")

# Test 2: Explicit acquire/release
print("\n2. Testing Explicit acquire/release...")
if coordinator.try_acquire("cleanup-job", ttl=120):
    print(f"   ✓ Acquired lease for 'cleanup-job'")
    print(f"   → Owner: {coordinator.owner_id}")
    time.sleep(1)
    coordinator.release("cleanup-job")
    print("   ✓ Released lease")
else:
    print("   ✗ Could not acquire lease")

# Test 3: Decorator
print("\n3. Testing Decorator pattern...")

@coordinator.singleton_task("scheduled-task", ttl=300)
def my_scheduled_task():
    print("   ✓ Task is running as singleton")
    print("   → Processing...")
    time.sleep(1)
    print("   ✓ Task completed")

my_scheduled_task()

print("\n" + "=" * 60)
print("All tests passed! ✨")
print("=" * 60)
