"""Basic usage examples for pglease."""

import time
from pglease import PGLease

pglease = PGLease("postgresql://user:password@localhost:5432/mydb")


# 1. Context Manager (recommended)
def context_manager_example():
    with pglease.acquire("daily-report", ttl=60) as acquired:
        if acquired:
            print("Generating report...")
            time.sleep(2)
            print("Done")
        else:
            print("Another worker is generating the report")


# 2. Explicit Control
def explicit_control_example():
    if pglease.try_acquire("data-sync", ttl=120):
        try:
            print("Syncing data...")
            time.sleep(2)
            print("Done")
        finally:
            pglease.release("data-sync")
    else:
        print("Another worker is syncing data")


# 3. Decorator (simplest)
@pglease.singleton_task("cleanup-job", ttl=300)
def cleanup_job():
    print("Running cleanup...")
    time.sleep(2)
    print("Done")
    return "success"


if __name__ == "__main__":
    try:
        context_manager_example()
        explicit_control_example()
        result = cleanup_job()
        print(f"Result: {result}")
    finally:
        pglease.close()
