"""Kubernetes deployment example."""

import os
import time
import logging
from pglease import Coordinator

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Get config from environment (K8s secrets/configmaps)
POSTGRES_URL = os.environ.get("DATABASE_URL", "postgresql://user:pass@postgres:5432/mydb")
POD_NAME = os.environ.get("HOSTNAME", "unknown-pod")

coordinator = Coordinator(POSTGRES_URL, owner_id=POD_NAME, heartbeat_interval=10)


@coordinator.singleton_task("database-migration", ttl=300)
def run_database_migration():
    """Run on startup - only one pod executes."""
    logger.info("Starting database migration...")
    time.sleep(5)
    logger.info("Migration completed")


@coordinator.singleton_task("hourly-aggregation", ttl=600)
def hourly_aggregation():
    """Scheduled by cron in each pod, but only one executes."""
    logger.info("Starting hourly aggregation...")
    time.sleep(30)
    logger.info("Aggregation completed")


def process_queue_item(item_id):
    """Process queue items with distributed locking."""
    task_name = f"queue-item-{item_id}"
    
    if coordinator.try_acquire(task_name, ttl=300):
        try:
            logger.info(f"Processing item {item_id}...")
            time.sleep(2)
            logger.info(f"Completed item {item_id}")
        finally:
            coordinator.release(task_name)
    else:
        logger.info(f"Item {item_id} being processed by another worker")


if __name__ == "__main__":
    logger.info(f"Starting worker: {POD_NAME}")
    
    try:
        run_database_migration()
        logger.info("Application running...")
        
        hourly_aggregation()
        
        for i in range(3):
            process_queue_item(f"item-{i}")
        
        logger.info("Worker ready")
        time.sleep(3600)
        
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    finally:
        coordinator.close()
