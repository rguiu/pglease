"""
Example: Scheduled Tasks

Demonstrates using pglease with APScheduler for distributed scheduled tasks.
"""

import time
import logging
from datetime import datetime
from apscheduler.schedulers.background import BackgroundScheduler
from pglease import PGLease

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize pglease
POSTGRES_URL = "postgresql://user:password@localhost:5432/mydb"
pglease = PGLease(POSTGRES_URL)

# Initialize scheduler
scheduler = BackgroundScheduler()


@pglease.singleton_task("hourly-cleanup", ttl=300)
def hourly_cleanup():
    """
    Cleanup task that runs every hour, but only on one worker.
    
    All workers schedule this task, but pglease ensures only one executes.
    """
    logger.info("Running hourly cleanup...")
    time.sleep(5)  # Simulate cleanup work
    logger.info("Hourly cleanup completed")


@pglease.singleton_task("daily-report", ttl=600)
def daily_report():
    """
    Generate daily report.
    
    Long-running task with heartbeat keeping the lease alive.
    """
    logger.info("Generating daily report...")
    time.sleep(30)  # Simulate report generation
    logger.info("Daily report completed")


def backup_database():
    """
    Database backup with explicit lock control.
    
    Shows how to check if task is already running before scheduling.
    """
    task_name = "database-backup"
    
    # Check if backup is already running
    lease = coordinator.get_lease(task_name)
    if lease and not lease.is_expired():
        logger.info(f"Backup already running by {lease.owner_id}, skipping")
        return
    
    # Try to acquire and run backup
    if coordinator.try_acquire(task_name, ttl=1800):  # 30 minute TTL
        try:
            logger.info("Starting database backup...")
            time.sleep(10)  # Simulate backup
            logger.info("Database backup completed")
        finally:
            coordinator.release(task_name)
    else:
        logger.info("Another worker started backup while we were checking")


def main():
    """Set up scheduled tasks."""
    logger.info("Starting scheduler...")
    
    # Schedule tasks (all workers schedule these, but only one executes)
    scheduler.add_job(
        hourly_cleanup,
        'interval',
        hours=1,
        id='hourly_cleanup',
    )
    
    scheduler.add_job(
        daily_report,
        'cron',
        hour=0,
        minute=0,
        id='daily_report',
    )
    
    scheduler.add_job(
        backup_database,
        'interval',
        hours=6,
        id='database_backup',
    )
    
    scheduler.start()
    
    try:
        logger.info("Scheduler running. Press Ctrl+C to exit.")
        # Keep main thread alive
        while True:
            time.sleep(1)
    
    except KeyboardInterrupt:
        logger.info("Shutting down...")
        scheduler.shutdown()
        coordinator.close()


if __name__ == "__main__":
    main()
