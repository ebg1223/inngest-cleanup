#!/usr/bin/env python3
"""
Simplified Inngest cleanup script for PostgreSQL.
Deletes old function runs, unused events, and empty event batches.
"""

import os
import sys
import time
import logging
import signal
import psycopg2
from datetime import datetime, timedelta
from contextlib import contextmanager

# Configuration from environment variables
DB_URL = os.getenv('DB_URL')
RETENTION_DAYS = int(os.getenv('RETENTION_DAYS', '30'))
BATCH_SIZE = int(os.getenv('BATCH_SIZE', '5000'))
SLEEP_SECONDS = float(os.getenv('SLEEP_SECONDS', '1.0'))
RUN_INTERVAL_SECONDS = int(os.getenv('RUN_INTERVAL_SECONDS', '3600'))  # 1 hour default
DRY_RUN = os.getenv('DRY_RUN', 'false').lower() == 'true'

# Global state
should_exit = False

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger('inngest-cleanup')


def signal_handler(signum, frame):
    """Handle termination signals gracefully."""
    global should_exit
    logger.info(f"Received signal {signum}, will exit after current batch")
    should_exit = True


@contextmanager
def db_retry(max_retries=3, retry_delay=5):
    """Retry database operations with exponential backoff."""
    retry_count = 0
    while True:
        try:
            yield
            break
        except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
            retry_count += 1
            if retry_count > max_retries:
                logger.error(f"Max retries exceeded: {e}")
                raise
            backoff = retry_delay * (2 ** (retry_count - 1))
            logger.warning(f"Database error: {e}, retrying in {backoff}s (attempt {retry_count}/{max_retries})")
            time.sleep(backoff)


class Cleaner:
    def __init__(self, db_url, retention_days, batch_size, sleep_seconds, dry_run=False):
        self.db_url = db_url
        self.retention_days = retention_days
        self.batch_size = batch_size
        self.sleep_seconds = sleep_seconds
        self.dry_run = dry_run
        self.conn = None
        self.cutoff_date = datetime.utcnow() - timedelta(days=retention_days)
        
        # Initialize database connection
        self.connect_db()

    def connect_db(self):
        """Connect to the PostgreSQL database."""
        try:
            self.conn = psycopg2.connect(self.db_url)
            self.conn.autocommit = False
            logger.info("Connected to database")
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            raise

    def close(self):
        """Close database connection."""
        if self.conn:
            self.conn.close()
            logger.info("Database connection closed")

    def cleanup_function_runs(self):
        """Delete old function runs in batches."""
        if self.dry_run:
            with db_retry():
                with self.conn.cursor() as cur:
                    # Count how many function runs would be deleted
                    cur.execute("""
                        SELECT COUNT(*) 
                        FROM function_runs
                        WHERE run_started_at < %s
                    """, (self.cutoff_date,))
                    count = cur.fetchone()[0]
                    logger.info(f"[DRY RUN] Would delete {count} function runs older than {self.cutoff_date}")
                    return count
            return 0

        total_deleted = 0
        
        with db_retry():
            with self.conn.cursor() as cur:
                while True:
                    if should_exit:
                        break
                        
                    # Begin transaction
                    self.conn.rollback()  # Clear any previous transaction
                    
                    # Get batch of old function runs to delete
                    cur.execute("""
                        WITH to_delete AS (
                            SELECT run_id
                            FROM function_runs
                            WHERE run_started_at < %s
                            ORDER BY run_started_at
                            LIMIT %s
                        )
                        DELETE FROM function_runs
                        WHERE run_id IN (SELECT run_id FROM to_delete)
                        RETURNING run_id
                    """, (self.cutoff_date, self.batch_size))
                    
                    deleted_count = cur.rowcount
                    if deleted_count == 0:
                        logger.info("No more old function runs to delete")
                        break
                        
                    self.conn.commit()
                    total_deleted += deleted_count
                    logger.info(f"Deleted {deleted_count} function runs (total: {total_deleted})")
                    time.sleep(self.sleep_seconds)
        
        return total_deleted

    def cleanup_events(self):
        """Delete events with no function run references or that are too old."""
        if self.dry_run:
            with db_retry():
                with self.conn.cursor() as cur:
                    # Count how many events would be deleted
                    cur.execute("""
                        SELECT COUNT(*)
                        FROM events e
                        WHERE e.received_at < %s
                        AND NOT EXISTS (
                            SELECT 1 
                            FROM function_runs fr 
                            WHERE fr.event_id = e.internal_id
                        )
                    """, (self.cutoff_date,))
                    count = cur.fetchone()[0]
                    logger.info(f"[DRY RUN] Would delete {count} unused events older than {self.cutoff_date}")
                    return count
            return 0

        total_deleted = 0
        
        with db_retry():
            with self.conn.cursor() as cur:
                while True:
                    if should_exit:
                        break
                        
                    # Begin transaction
                    self.conn.rollback()  # Clear any previous transaction
                    
                    # Get batch of events to delete - either they're old enough AND have no function run reference
                    cur.execute("""
                        WITH to_delete AS (
                            SELECT e.internal_id
                            FROM events e
                            WHERE e.received_at < %s
                            AND NOT EXISTS (
                                SELECT 1 
                                FROM function_runs fr 
                                WHERE fr.event_id = e.internal_id
                            )
                            LIMIT %s
                        )
                        DELETE FROM events
                        WHERE internal_id IN (SELECT internal_id FROM to_delete)
                        RETURNING internal_id
                    """, (self.cutoff_date, self.batch_size))
                    
                    deleted_count = cur.rowcount
                    if deleted_count == 0:
                        logger.info("No more unused events to delete")
                        break
                        
                    self.conn.commit()
                    total_deleted += deleted_count
                    logger.info(f"Deleted {deleted_count} unused events (total: {total_deleted})")
                    time.sleep(self.sleep_seconds)
        
        return total_deleted

    def cleanup_event_batches(self):
        """Delete event batches with no event IDs."""
        if self.dry_run:
            with db_retry():
                with self.conn.cursor() as cur:
                    # Count how many event batches would be deleted
                    cur.execute("""
                        SELECT COUNT(*) 
                        FROM event_batches 
                        WHERE event_ids IS NULL OR event_ids = '\\x'
                    """)
                    count = cur.fetchone()[0]
                    logger.info(f"[DRY RUN] Would delete {count} empty event batches")
                    return count
            return 0

        total_deleted = 0
        
        with db_retry():
            with self.conn.cursor() as cur:
                while True:
                    if should_exit:
                        break
                        
                    # Begin transaction
                    self.conn.rollback()  # Clear any previous transaction
                    
                    # Get batch of empty event batches to delete
                    cur.execute("""
                        WITH to_delete AS (
                            SELECT id 
                            FROM event_batches 
                            WHERE event_ids IS NULL OR event_ids = '\\x'
                            LIMIT %s
                        )
                        DELETE FROM event_batches
                        WHERE id IN (SELECT id FROM to_delete)
                        RETURNING id
                    """, (self.batch_size,))
                    
                    deleted_count = cur.rowcount
                    if deleted_count == 0:
                        logger.info("No more empty event batches to delete")
                        break
                        
                    self.conn.commit()
                    total_deleted += deleted_count
                    logger.info(f"Deleted {deleted_count} empty event batches (total: {total_deleted})")
                    time.sleep(self.sleep_seconds)
        
        return total_deleted

    def run_cleanup(self):
        """Run the complete cleanup process once."""
        logger.info("Starting cleanup with cutoff date: %s", self.cutoff_date)
        
        try:
            if self.dry_run:
                logger.info("[DRY RUN] Counting rows that would be deleted (not actually deleting)")
            
            # Step 1: Delete old function runs
            logger.info("Cleaning up old function runs...")
            function_runs_deleted = self.cleanup_function_runs()
            
            # Step 2: Delete unused events
            logger.info("Cleaning up unused events...")
            events_deleted = self.cleanup_events()
            
            # Step 3: Delete empty event batches
            logger.info("Cleaning up empty event batches...")
            batches_deleted = self.cleanup_event_batches()
            
            if self.dry_run:
                logger.info(
                    "[DRY RUN] Cleanup completed: would delete %d function runs, %d events, %d event batches",
                    function_runs_deleted or 0,
                    events_deleted or 0,
                    batches_deleted or 0
                )
            else:
                logger.info(
                    "Cleanup completed: %d function runs, %d events, %d event batches deleted",
                    function_runs_deleted or 0,
                    events_deleted or 0,
                    batches_deleted or 0
                )
            
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")
            self.conn.rollback()
            raise


def main():
    """Main function that sets up signal handlers and runs the cleanup loop."""
    if not DB_URL:
        logger.error("DATABASE_URL environment variable is required")
        sys.exit(1)
        
    # Setup signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    logger.info("Starting Inngest cleanup service")
    logger.info(f"Configuration: retention_days={RETENTION_DAYS}, batch_size={BATCH_SIZE}, "
                f"sleep_seconds={SLEEP_SECONDS}, dry_run={DRY_RUN}")
    
    cleaner = None
    try:
        cleaner = Cleaner(
            db_url=DB_URL,
            retention_days=RETENTION_DAYS,
            batch_size=BATCH_SIZE,
            sleep_seconds=SLEEP_SECONDS,
            dry_run=DRY_RUN
        )
        
        # Main loop
        while not should_exit:
            start_time = time.time()
            
            cleaner.run_cleanup()
            
            # Sleep until next run interval, but check should_exit frequently
            elapsed = time.time() - start_time
            remaining = max(0, RUN_INTERVAL_SECONDS - elapsed)
            logger.info(f"Cleanup cycle completed in {elapsed:.2f}s, next run in {remaining:.2f}s")
            
            # Check for exit signal every second
            for _ in range(int(remaining)):
                if should_exit:
                    break
                time.sleep(1)
            
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)
    finally:
        if cleaner:
            cleaner.close()
        logger.info("Cleanup service stopped")


if __name__ == "__main__":
    main()
