#!/usr/bin/env python3

import argparse
import logging
import psycopg2
import psycopg2.extras
import signal
import sys
import time
from contextlib import contextmanager
from datetime import datetime, timedelta, UTC

class GracefulExit(Exception):
    pass

class EventCleaner:
    def __init__(
        self,
        db_url,
        retention_days,
        batch_size=5000,
        sleep_seconds=1,
        dry_run=False,
        max_retries=3,
        retry_delay=5
    ):
        self.db_url = db_url
        self.retention_days = retention_days
        self.batch_size = batch_size
        self.sleep_seconds = sleep_seconds
        self.dry_run = dry_run
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.conn = None
        self.logger = None
        self.should_exit = False
        
        self.setup_logging()
        self.setup_signal_handlers()
        self.connect_db()
        self.setup_indexes()

    def setup_logging(self):
        self.logger = logging.getLogger('event_cleaner')
        self.logger.setLevel(logging.INFO)
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(
            logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        )
        self.logger.addHandler(handler)

    def setup_signal_handlers(self):
        def signal_handler(signum, frame):
            self.logger.info(f"Received signal {signum}. Initiating graceful shutdown...")
            self.should_exit = True
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

    @contextmanager
    def db_retry(self):
        """Context manager for database operations with retry logic"""
        retries = 0
        while True:
            try:
                yield
                break
            except psycopg2.Error as e:
                retries += 1
                if retries >= self.max_retries:
                    self.logger.error(f"Max retries ({self.max_retries}) reached. Error: {e}")
                    raise
                self.logger.warning(f"Database error (attempt {retries}/{self.max_retries}): {e}")
                self.logger.info(f"Retrying in {self.retry_delay} seconds...")
                time.sleep(self.retry_delay)
                self.reconnect_db()

    def connect_db(self):
        self.logger.info("Connecting to database")
        self.conn = psycopg2.connect(
            self.db_url,
            cursor_factory=psycopg2.extras.DictCursor
        )
        # Enable autocommit for better performance with SKIP LOCKED
        self.conn.set_session(autocommit=True)

    def reconnect_db(self):
        """Safely reconnect to the database"""
        if self.conn:
            try:
                self.conn.close()
            except:
                pass
        self.connect_db()

    def setup_indexes(self):
        """Create necessary indexes if they don't exist"""
        self.logger.info("Ensuring required indexes exist")
        index_queries = [
            """
            CREATE INDEX IF NOT EXISTS idx_events_received_at 
            ON events (received_at, internal_id);
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_function_runs_event_id 
            ON function_runs (event_id);
            """
        ]

        with self.db_retry():
            with self.conn.cursor() as cur:
                for query in index_queries:
                    cur.execute(query)
                    self.logger.info("Created index if it didn't exist")

    def get_stats(self):
        """Get statistics about the cleanup operation"""
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT 
                    COUNT(*) as total_events,
                    COUNT(*) FILTER (WHERE received_at < %s) as eligible_events,
                    MIN(received_at) as oldest_event,
                    MAX(received_at) as newest_event
                FROM events
            """, (datetime.now(UTC) - timedelta(days=self.retention_days),))
            stats = cur.fetchone()
            return {
                'total_events': stats[0],
                'eligible_events': stats[1],
                'oldest_event': stats[2],
                'newest_event': stats[3]
            }

    def cleanup(self):
        """Clean up all old events that are no longer needed"""
        self.logger.info(f"Starting cleanup with retention period of {self.retention_days} days")
        
        # Print initial statistics
        stats = self.get_stats()
        self.logger.info(
            f"Initial state: {stats['total_events']:,} total events, "
            f"{stats['eligible_events']:,} eligible for cleanup"
        )
        
        cutoff_date = datetime.now(UTC) - timedelta(days=self.retention_days)
        total_deleted = 0
        start_time = time.time()
        
        try:
            while not self.should_exit:
                with self.db_retry():
                    with self.conn.cursor() as cur:
                        if self.dry_run:
                            query = """
                            WITH candidate_events AS (
                                SELECT 1
                                FROM events e
                                WHERE e.received_at < %s
                                AND NOT EXISTS (
                                    SELECT 1 FROM function_runs fr 
                                    WHERE fr.event_id = e.internal_id
                                )
                                AND NOT EXISTS (
                                    SELECT 1 FROM event_batches eb 
                                    WHERE position(encode(e.internal_id, 'hex') in encode(eb.event_ids, 'hex')) > 0
                                )
                                LIMIT %s
                            )
                            SELECT COUNT(*) FROM candidate_events;
                            """
                        else:
                            query = """
                            WITH deleted_events AS (
                                DELETE FROM events e
                                WHERE e.received_at < %s
                                AND NOT EXISTS (
                                    SELECT 1 FROM function_runs fr 
                                    WHERE fr.event_id = e.internal_id
                                )
                                AND NOT EXISTS (
                                    SELECT 1 FROM event_batches eb 
                                    WHERE position(encode(e.internal_id, 'hex') in encode(eb.event_ids, 'hex')) > 0
                                )
                                LIMIT %s
                                RETURNING 1
                            )
                            SELECT COUNT(*) FROM deleted_events;
                            """
                        
                        cur.execute(query, (cutoff_date, self.batch_size))
                        count = cur.fetchone()[0]

                        if not count:
                            self.logger.info("No more events to clean up")
                            break

                        if self.dry_run:
                            self.logger.info(f"Would delete {count} events")
                        else:
                            total_deleted += count
                            elapsed = time.time() - start_time
                            rate = total_deleted / elapsed if elapsed > 0 else 0
                            self.logger.info(
                                f"Deleted {count} events "
                                f"(Total: {total_deleted:,}, Rate: {rate:.1f} events/sec)"
                            )

                        if self.sleep_seconds > 0:
                            time.sleep(self.sleep_seconds)

        except GracefulExit:
            self.logger.info("Gracefully shutting down...")
        
        if total_deleted > 0:
            elapsed = time.time() - start_time
            final_rate = total_deleted / elapsed if elapsed > 0 else 0
            self.logger.info(
                f"Cleanup completed: {total_deleted:,} events deleted in "
                f"{elapsed:.1f} seconds ({final_rate:.1f} events/sec)"
            )

    def close(self):
        if self.conn:
            self.conn.close()

def main():
    parser = argparse.ArgumentParser(description='Clean up old events from the database')
    parser.add_argument('--db-url', required=True, help='Database connection URL')
    parser.add_argument('--retention-days', type=int, default=3, help='Number of days to retain events')
    parser.add_argument('--batch-size', type=int, default=5000, help='Number of events to process in each batch')
    parser.add_argument('--sleep-seconds', type=float, default=0.1, help='Seconds to sleep between batches')
    parser.add_argument('--dry-run', action='store_true', help='Print what would be done without making changes')
    parser.add_argument('--max-retries', type=int, default=3, help='Maximum number of retries for database operations')
    parser.add_argument('--retry-delay', type=int, default=5, help='Delay in seconds between retries')

    args = parser.parse_args()

    cleaner = EventCleaner(
        db_url=args.db_url,
        retention_days=args.retention_days,
        batch_size=args.batch_size,
        sleep_seconds=args.sleep_seconds,
        dry_run=args.dry_run,
        max_retries=args.max_retries,
        retry_delay=args.retry_delay
    )

    try:
        cleaner.cleanup()
    finally:
        cleaner.close()

if __name__ == '__main__':
    main()
