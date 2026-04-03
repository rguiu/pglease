"""
Example: Hybrid Backend for Fast Failover

Demonstrates the hybrid backend that combines advisory locks with lease tables
for both fast failover and observability.
"""

import time
import logging
from pglease import PGLease, HybridPostgresBackend

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

POSTGRES_URL = "postgresql://user:password@localhost:5432/mydb"


def example_fast_failover():
    """
    Demonstrate fast failover with hybrid backend.
    
    If a worker crashes, the advisory lock is immediately released,
    allowing another worker to take over instantly.
    """
    print("Example: Fast Failover with Hybrid Backend")
    print("="*60)
    
    # Use hybrid backend for fast failover
    backend = HybridPostgresBackend(POSTGRES_URL)
    pglease = PGLease(backend, owner_id="worker-1")
    
    try:
        # Acquire critical task
        if pglease.try_acquire("critical-task", ttl=60):
            logger.info("✓ Acquired critical task")
            
            # Simulate long-running work
            logger.info("Processing critical operation...")
            time.sleep(5)
            
            # If this worker crashes here, advisory lock releases instantly
            # Another worker can acquire immediately (no TTL wait)
            
            logger.info("✓ Critical operation completed")
            pglease.release("critical-task")
        else:
            logger.info("✗ Task held by another worker")
    
    finally:
        pglease.close()


def example_connection_monitoring():
    """
    Demonstrate connection health monitoring via advisory locks.
    
    The hybrid backend verifies advisory lock during heartbeat,
    detecting connection loss immediately.
    """
    print("\nExample: Connection Health Monitoring")
    print("="*60)
    
    backend = HybridPostgresBackend(POSTGRES_URL)
    pglease = PGLease(backend, owner_id="worker-2")
    
    try:
        if pglease.try_acquire("monitored-task", ttl=120):
            logger.info("✓ Task acquired with connection monitoring")
            
            # Heartbeat will verify:
            # 1. Advisory lock still held (connection alive)
            # 2. Lease table expiry updated
            
            time.sleep(15)  # Heartbeat happens in background
            
            # If connection was lost, heartbeat would fail and task stops
            logger.info("✓ Connection healthy, task continuing")
            
            pglease.release("monitored-task")
    
    finally:
        pglease.close()


def example_observability_with_speed():
    """
    Demonstrate getting both observability and fast failover.
    """
    print("\nExample: Observability + Fast Failover")
    print("="*60)
    
    backend = HybridPostgresBackend(POSTGRES_URL)
    pglease = PGLease(backend, owner_id="worker-3")
    
    try:
        if pglease.try_acquire("observable-task", ttl=60):
            logger.info("✓ Task acquired (hybrid mode)")
            
            # You get:
            # 1. Advisory lock (fast failover if we crash)
            # 2. Lease table entry (query for observability)
            
            # Query lease table for metadata
            lease = pglease.get_lease("observable-task")
            if lease:
                logger.info(f"  Owner: {lease.owner_id}")
                logger.info(f"  Acquired: {lease.acquired_at}")
                logger.info(f"  Expires: {lease.expires_at}")
                logger.info(f"  Time remaining: {lease.time_remaining():.1f}s")
            
            time.sleep(3)
            pglease.release("observable-task")
            logger.info("✓ Task completed and released")
    
    finally:
        pglease.close()


def compare_backends():
    """
    Compare standard vs hybrid backend behavior.
    """
    print("\nComparison: Standard vs Hybrid Backend")
    print("="*60)
    
    print("\nStandard Backend (PostgresBackend):")
    print("  ✓ Simple implementation")
    print("  ✓ Rich metadata and observability")
    print("  ✓ Handles transient connection issues")
    print("  ⚠ Failover: 10-30s (waits for TTL)")
    print("  📊 Use for: Migrations, scheduled jobs, non-critical tasks")
    
    print("\nHybrid Backend (HybridPostgresBackend):")
    print("  ✓ Fast failover (<1s)")
    print("  ✓ Rich metadata and observability")
    print("  ✓ Connection health monitoring")
    print("  ✓ Double safety (advisory + TTL)")
    print("  ⚠ Slightly more complex")
    print("  📊 Use for: Critical tasks, leader election, fast recovery needed")


def main():
    """Run all examples."""
    try:
        example_fast_failover()
        time.sleep(1)
        
        example_connection_monitoring()
        time.sleep(1)
        
        example_observability_with_speed()
        time.sleep(1)
        
        compare_backends()
        
    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)


if __name__ == "__main__":
    main()
