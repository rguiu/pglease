"""
Example: Multi-Worker Simulation

Simulates multiple workers competing for singleton tasks.
Useful for testing and understanding coordination behavior.
"""

import time
import threading
import logging
from pglease import Coordinator

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - [%(threadName)s] - %(message)s'
)

# Shared database for testing
POSTGRES_URL = "postgresql://user:password@localhost:5432/testdb"


def worker(worker_id: str, task_name: str, duration: int):
    """
    Worker function that tries to acquire and execute a task.
    
    Args:
        worker_id: Unique identifier for this worker
        task_name: Name of the task to coordinate on
        duration: How long to hold the lock
    """
    logger = logging.getLogger(f"worker-{worker_id}")
    
    # Create coordinator for this worker
    coordinator = Coordinator(POSTGRES_URL, owner_id=worker_id)
    
    try:
        logger.info(f"Attempting to acquire task: {task_name}")
        
        with coordinator.acquire(task_name, ttl=duration) as acquired:
            if acquired:
                logger.info(f"✓ Acquired {task_name} - executing...")
                time.sleep(duration)
                logger.info(f"✓ Completed {task_name}")
            else:
                logger.info(f"✗ Failed to acquire {task_name} - another worker has it")
    
    finally:
        coordinator.close()


def simulate_concurrent_workers():
    """
    Simulate multiple workers trying to acquire the same task simultaneously.
    
    Only one should succeed at a time.
    """
    print("\n" + "="*60)
    print("Simulation: Concurrent Workers")
    print("="*60 + "\n")
    
    threads = []
    
    # Start 5 workers simultaneously
    for i in range(5):
        t = threading.Thread(
            target=worker,
            args=(f"worker-{i}", "shared-task", 5),
            name=f"worker-{i}"
        )
        threads.append(t)
        t.start()
    
    # Wait for all workers
    for t in threads:
        t.join()
    
    print("\nResult: Only one worker should have executed the task")


def simulate_sequential_execution():
    """
    Simulate workers taking turns executing a task.
    
    As each worker finishes, the next can acquire the lock.
    """
    print("\n" + "="*60)
    print("Simulation: Sequential Execution")
    print("="*60 + "\n")
    
    threads = []
    
    # Start 3 workers with staggered delays
    for i in range(3):
        t = threading.Thread(
            target=worker,
            args=(f"seq-worker-{i}", "sequential-task", 3),
            name=f"seq-worker-{i}"
        )
        threads.append(t)
        t.start()
        time.sleep(4)  # Start next worker after previous finishes
    
    # Wait for all workers
    for t in threads:
        t.join()
    
    print("\nResult: Each worker should have executed the task in sequence")


def simulate_lease_expiry():
    """
    Simulate a worker dying and another taking over the expired lease.
    """
    print("\n" + "="*60)
    print("Simulation: Lease Expiry and Takeover")
    print("="*60 + "\n")
    
    logger = logging.getLogger("simulation")
    
    # Worker 1 acquires with short TTL and "dies" (doesn't renew)
    coord1 = Coordinator(POSTGRES_URL, owner_id="dying-worker")
    coord1.heartbeat_manager.stop_all()  # Stop heartbeat to simulate death
    
    if coord1.try_acquire("expiry-task", ttl=3):
        logger.info("Worker 1 acquired lease (will die without releasing)")
        # Don't release - simulate crash
    
    # Wait for lease to expire
    logger.info("Waiting for lease to expire...")
    time.sleep(4)
    
    # Worker 2 should be able to take over
    coord2 = Coordinator(POSTGRES_URL, owner_id="recovery-worker")
    if coord2.try_acquire("expiry-task", ttl=60):
        logger.info("✓ Worker 2 took over expired lease")
        coord2.release("expiry-task")
    else:
        logger.error("✗ Worker 2 failed to acquire expired lease")
    
    coord1.close()
    coord2.close()
    
    print("\nResult: Second worker should have taken over after lease expired")


def main():
    """Run all simulations."""
    simulate_concurrent_workers()
    time.sleep(2)
    
    simulate_sequential_execution()
    time.sleep(2)
    
    simulate_lease_expiry()


if __name__ == "__main__":
    main()
