#!/usr/bin/env python3

import argparse
import logging
import psycopg2
import psycopg2.extras
import signal
import sys
import time
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
from contextlib import contextmanager
from datetime import datetime, timedelta, UTC
import os
import uuid

# Global variables for health status
db_is_healthy = True
db_last_error = None
db_health_lock = threading.Lock()

class GracefulExit(Exception):
    pass

class HealthCheckHandler(BaseHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        self.health_status = kwargs.pop('health_status', lambda: True)
        super().__init__(*args, **kwargs)
    
    def do_GET(self):
        global db_last_error
        if self.path == '/health' or self.path == '/':
            # Return 200 OK if the service is healthy
            if self.health_status():
                self.send_response(200)
                self.send_header('Content-type', 'text/plain')
                self.end_headers()
                self.wfile.write(b'OK')
            else:
                with db_health_lock:
                    error_msg = db_last_error if db_last_error else "Service is shutting down"
                
                self.send_response(503)
                self.send_header('Content-type', 'text/plain')
                self.end_headers()
                self.wfile.write(f"Service Unavailable: {error_msg}".encode('utf-8'))
        else:
            self.send_response(404)
            self.send_header('Content-type', 'text/plain')
            self.end_headers()
            self.wfile.write(b'Not Found')
    
    # Silence log messages for healthcheck requests
    def log_message(self, format, *args):
        return

def health_server_factory(health_status):
    def handler(*args, **kwargs):
        return HealthCheckHandler(*args, health_status=health_status, **kwargs)
    return handler

def start_health_server(port, health_status_func, logger):
    """Start a health check server in a separate thread"""
    handler = health_server_factory(health_status_func)
    server = HTTPServer(('0.0.0.0', port), handler)
    
    logger.info(f"Starting health check server on port {port}")
    
    # Run the server in a daemon thread
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    
    return server

class EventCleaner:
    def __init__(
        self,
        db_url,
        retention_days,
        batch_size=5000,
        sleep_seconds=1,
        dry_run=False,
        max_retries=3,
        retry_delay=5,
        logger=None,
        should_exit_callback=None
    ):
        self.db_url = db_url
        self.retention_days = retention_days
        self.batch_size = batch_size
        self.sleep_seconds = sleep_seconds
        self.dry_run = dry_run
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.conn = None
        self.logger = logger
        self.should_exit_callback = should_exit_callback
        self.instance_id = str(uuid.uuid4())[:8]  # Short unique ID for logging
        
        if self.logger is None:
            self.setup_logging()
        
        self.logger.info(f"Initializing cleaner instance {self.instance_id}")
        self.connect_db()
        self.setup_indexes()

    def setup_logging(self):
        self.logger = logging.getLogger('event_cleaner')
        self.logger.setLevel(logging.INFO)
        # Check if the logger already has handlers to avoid duplicates
        if not self.logger.handlers:
            handler = logging.StreamHandler(sys.stdout)
            handler.setFormatter(
                logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
            )
            self.logger.addHandler(handler)

    def check_should_exit(self):
        if self.should_exit_callback and self.should_exit_callback():
            return True
        return False

    def setup_signal_handlers(self):
        # Removed - signal handlers are now managed at the main level
        pass
        
    @contextmanager
    def db_retry(self):
        """Context manager for database operations with retry logic"""
        global db_is_healthy, db_last_error
        retries = 0
        while True:
            try:
                yield
                # Update health status on success
                with db_health_lock:
                    db_is_healthy = True
                    db_last_error = None
                return  # Use return instead of break to fix generator issues
            except psycopg2.Error as e:
                retries += 1
                # Update health status on error
                with db_health_lock:
                    if retries >= self.max_retries:
                        db_is_healthy = False
                        db_last_error = str(e)
                
                if retries >= self.max_retries:
                    self.logger.error(f"Max retries ({self.max_retries}) reached. Error: {e}")
                    raise
                self.logger.warning(f"Database error (attempt {retries}/{self.max_retries}): {e}")
                self.logger.info(f"Retrying in {self.retry_delay} seconds...")
                time.sleep(self.retry_delay)
                self.reconnect_db()

    def connect_db(self):
        global db_is_healthy, db_last_error
        self.logger.info(f"Connecting to database (instance {self.instance_id})")
        try:
            # Connect without statement timeout initially, set application name as a parameter
            app_name = f"inngest-cleanup-{self.instance_id}"
            self.conn = psycopg2.connect(
                self.db_url,
                cursor_factory=psycopg2.extras.DictCursor,
                application_name=app_name
            )
            # Enable autocommit for better performance
            self.conn.set_session(autocommit=True)
            
            # Test the connection with a simple query
            with self.conn.cursor() as cur:
                cur.execute("SELECT 1")
                
            # Update health status
            with db_health_lock:
                db_is_healthy = True
                db_last_error = None
                
            self.logger.debug(f"Successfully connected to database (instance {self.instance_id})")
        except Exception as e:
            # Update health status
            with db_health_lock:
                db_is_healthy = False
                db_last_error = str(e)
            self.logger.error(f"Database connection error: {e}")
            raise

    def set_statement_timeout(self, timeout_ms=120000):
        """Set the statement timeout for the current connection"""
        if self.conn:
            with self.conn.cursor() as cur:
                cur.execute(f"SET statement_timeout = {timeout_ms}")
            self.logger.debug(f"Set statement timeout to {timeout_ms}ms")

    def reconnect_db(self):
        """Safely reconnect to the database"""
        self.logger.info(f"Reconnecting to database (instance {self.instance_id})")
        if self.conn:
            try:
                # Cancel any in-progress queries first
                # Create a new connection just for the cancel
                cancel_conn = None
                try:
                    # Connect with a very short timeout to quickly cancel query
                    cancel_conn = psycopg2.connect(
                        self.db_url,
                        connect_timeout=5,
                        application_name=f"inngest-cleanup-cancel-{self.instance_id}"
                    )
                    with cancel_conn.cursor() as cur:
                        # Find our own queries and cancel them
                        cur.execute("""
                            SELECT pid FROM pg_stat_activity 
                            WHERE backend_type = 'client backend'
                            AND application_name ~ 'inngest-cleanup-.*'
                            AND state = 'active'
                            AND pid <> pg_backend_pid()
                        """)
                        for row in cur.fetchall():
                            backend_pid = row[0]
                            self.logger.debug(f"Canceling query in process {backend_pid}")
                            try:
                                cur.execute(f"SELECT pg_cancel_backend({backend_pid})")
                            except Exception as e:
                                self.logger.warning(f"Error canceling query: {e}")
                except Exception as e:
                    self.logger.warning(f"Could not cancel queries: {e}")
                finally:
                    if cancel_conn:
                        try:
                            cancel_conn.close()
                        except:
                            pass
                
                # Now close our main connection
                self.conn.close()
            except Exception as e:
                self.logger.warning(f"Error closing old connection: {e}")
        
        self.conn = None
        self.connect_db()

    def setup_indexes(self):
        """Create necessary indexes if they don't exist"""
        self.logger.info("Checking for required indexes")
        
        required_indexes = {
            "idx_events_received_at": "events",
            "idx_function_runs_event_id": "function_runs"
        }
        
        missing_indexes = []
        
        # First check which indexes already exist
        check_start = time.time()
        try:
            with self.db_retry():
                # Set a short timeout for the index existence check
                self.set_statement_timeout(10000)  # 10 second timeout
                
                with self.conn.cursor() as cur:
                    for index_name, table in required_indexes.items():
                        self.logger.debug(f"Checking if index {index_name} exists")
                        query_start = time.time()
                        cur.execute(
                            "SELECT 1 FROM pg_indexes WHERE indexname = %s",
                            (index_name,)
                        )
                        exists = cur.fetchone() is not None
                        query_end = time.time()
                        self.logger.debug(f"Index {index_name} {'exists' if exists else 'does not exist'} (took {query_end - query_start:.2f}s)")
                        if not exists:
                            missing_indexes.append((index_name, table))
        except Exception as e:
            self.logger.warning(f"Error checking indexes: {e}")
            self.logger.info("Proceeding without creating indexes")
            return
            
        check_end = time.time()
        self.logger.debug(f"Completed index existence check in {check_end - check_start:.2f}s")
        
        # Only create indexes that are missing
        if missing_indexes:
            self.logger.info(f"Creating {len(missing_indexes)} missing indexes")
            create_start = time.time()
            with self.db_retry():
                # Allow longer timeout for index creation
                self.set_statement_timeout(300000)  # 5 minute timeout
                
                with self.conn.cursor() as cur:
                    for index_name, table in missing_indexes:
                        self.logger.debug(f"Creating index {index_name}")
                        index_start = time.time()
                        if index_name == "idx_events_received_at":
                            cur.execute(
                                f"""
                                CREATE INDEX {index_name} 
                                ON {table} (received_at, internal_id);
                                """
                            )
                        elif index_name == "idx_function_runs_event_id":
                            cur.execute(
                                f"""
                                CREATE INDEX {index_name} 
                                ON {table} (event_id);
                                """
                            )
                        index_end = time.time()
                        self.logger.info(f"Created index {index_name} in {index_end - index_start:.2f}s")
            create_end = time.time()
            self.logger.debug(f"Completed index creation in {create_end - create_start:.2f}s")
        else:
            self.logger.info("All required indexes already exist")
            
        # Reset statement timeout to default
        self.set_statement_timeout()

    def get_stats(self):
        """Get statistics about the cleanup operation using a fast, non-blocking query"""
        self.logger.debug("Getting database statistics")
        cutoff_date = datetime.now(UTC) - timedelta(days=self.retention_days)
        
        with self.db_retry():
            with self.conn.cursor() as cur:
                # First get fast metadata-based stats
                self.logger.debug("Querying metadata for table size")
                cur.execute("""
                    SELECT 
                        reltuples::bigint as estimated_rows
                    FROM pg_class 
                    WHERE relname = 'events'
                """)
                result = cur.fetchone()
                estimated_total = int(result[0]) if result and result[0] else 0
                
                # Get min/max dates with a fast index-only scan
                self.logger.debug("Querying min/max dates")
                cur.execute("""
                    SELECT 
                        MIN(received_at) as oldest_event,
                        MAX(received_at) as newest_event
                    FROM events
                    WHERE received_at IS NOT NULL
                """)
                date_stats = cur.fetchone()
                
                # Get an estimate of eligible records with the FULL eligibility criteria 
                self.logger.debug("Estimating eligible records (with full criteria)")
                
                # Set a longer timeout for this query as it's more complex
                self.set_statement_timeout(30000)  # 30 seconds
                
                # Use the same conditions as the actual delete query
                cur.execute("""
                    SELECT COUNT(*) FROM (
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
                        LIMIT 1000
                    ) t
                """, (cutoff_date,))
                eligible_with_criteria = cur.fetchone()[0]
                
                # Also get a count of all events older than cutoff (just by date)
                self.logger.debug("Estimating records by date only")
                cur.execute("""
                    SELECT COUNT(*) FROM (
                        SELECT 1 
                        FROM events 
                        WHERE received_at < %s
                        LIMIT 1000
                    ) t
                """, (cutoff_date,))
                eligible_by_date = cur.fetchone()[0]
                
                # Reset the timeout
                self.set_statement_timeout()
                
                return {
                    'total_events': estimated_total,
                    'eligible_events': eligible_with_criteria,
                    'eligible_sample': eligible_with_criteria >= 1000, 
                    'eligible_by_date': eligible_by_date,
                    'eligible_by_date_sample': eligible_by_date >= 1000,
                    'oldest_event': date_stats[0],
                    'newest_event': date_stats[1]
                }

    def cleanup(self):
        """Clean up all old events that are no longer needed"""
        self.logger.info(f"Starting cleanup with retention period of {self.retention_days} days")
        
        try:
            # Print initial statistics
            try:
                stats = self.get_stats()
                by_date_text = (f"{stats['eligible_by_date']:,}+" if stats['eligible_by_date_sample'] 
                               else f"{stats['eligible_by_date']:,}")
                eligible_text = (f"{stats['eligible_events']:,}+" if stats['eligible_sample'] 
                                else f"{stats['eligible_events']:,}")
                
                self.logger.info(
                    f"Initial state: {stats['total_events']:,} total events, "
                    f"{by_date_text} older than cutoff, "
                    f"{eligible_text} eligible for cleanup"
                )
                
                if stats['eligible_by_date'] > stats['eligible_events']:
                    self.logger.info(
                        f"Note: Most old events are protected by references in function_runs or event_batches"
                    )
            except Exception as e:
                self.logger.warning(f"Unable to get initial stats: {e}")
            
            cutoff_date = datetime.now(UTC) - timedelta(days=self.retention_days)
            total_deleted = 0
            start_time = time.time()
            no_more_events = False
            
            # For dry run mode, we need to keep track of processed IDs
            processed_ids = set()
            batch_num = 0
            
            while not self.check_should_exit() and not no_more_events:
                try:
                    with self.db_retry():
                        # Set a longer statement timeout for delete operations
                        self.set_statement_timeout(120000)  # 2 minutes
                        
                        with self.conn.cursor() as cur:
                            batch_num += 1
                            self.logger.info(f"Processing events batch #{batch_num}")
                            
                            if self.dry_run:
                                self.logger.info("Running dry run query...")
                                
                                # If we've already processed some IDs, exclude them
                                if processed_ids:
                                    cur.execute("""
                                        SELECT e.internal_id
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
                                        AND e.internal_id NOT IN (SELECT unnest(%s))
                                        ORDER BY e.received_at
                                        LIMIT %s
                                    """, (cutoff_date, list(processed_ids), self.batch_size))
                                else:
                                    cur.execute("""
                                        SELECT e.internal_id
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
                                        ORDER BY e.received_at
                                        LIMIT %s
                                    """, (cutoff_date, self.batch_size))
                                
                                # Get the IDs and count
                                rows = cur.fetchall()
                                count = len(rows)
                                
                                # Add these IDs to our processed set
                                if count > 0:
                                    new_ids = {row[0] for row in rows}
                                    processed_ids.update(new_ids)
                                
                            else:
                                self.logger.info("Running delete query...")
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
                                    ORDER BY e.received_at
                                    LIMIT %s
                                    RETURNING 1
                                )
                                SELECT COUNT(*) FROM deleted_events;
                                """
                                
                                cur.execute(query, (cutoff_date, self.batch_size))
                                count = cur.fetchone()[0]

                            if not count:
                                self.logger.info("No more events to clean up")
                                no_more_events = True
                                break

                            if self.dry_run:
                                self.logger.info(f"Would delete {count} events (total processed so far: {len(processed_ids)})")
                            else:
                                total_deleted += count
                                elapsed = time.time() - start_time
                                rate = total_deleted / elapsed if elapsed > 0 else 0
                                self.logger.info(
                                    f"Deleted {count} events "
                                    f"(Total: {total_deleted:,}, Rate: {rate:.1f} events/sec)"
                                )

                            # If we didn't get a full batch, we're likely done
                            if count < self.batch_size:
                                self.logger.info("Received less than a full batch, likely done")
                                no_more_events = True

                            if self.sleep_seconds > 0 and not self.check_should_exit() and not no_more_events:
                                time.sleep(self.sleep_seconds)
                except Exception as e:
                    if self.check_should_exit():
                        break
                    self.logger.error(f"Error during cleanup batch: {e}")
                    # Wait before retrying to avoid tight loops
                    time.sleep(self.retry_delay)

        except GracefulExit:
            self.logger.info("Gracefully shutting down...")
        except Exception as e:
            self.logger.error(f"Unexpected error in cleanup: {e}")
        
        if total_deleted > 0 and not self.dry_run:
            elapsed = time.time() - start_time
            final_rate = total_deleted / elapsed if elapsed > 0 else 0
            self.logger.info(
                f"Cleanup completed: {total_deleted:,} events deleted in "
                f"{elapsed:.1f} seconds ({final_rate:.1f} events/sec)"
            )
        elif self.dry_run:
            self.logger.info(f"Events cleanup (dry run) completed: would delete {len(processed_ids):,} events")
            
        return total_deleted if not self.dry_run else len(processed_ids)

    def close(self):
        if self.conn:
            self.conn.close()

    def debug_event_references(self):
        """Debug method to check how many events are referenced in different tables"""
        self.logger.info("Analyzing event references (this may take a moment)...")
        cutoff_date = datetime.now(UTC) - timedelta(days=self.retention_days)
        
        with self.db_retry():
            with self.conn.cursor() as cur:
                # Set a longer timeout for these analysis queries
                self.set_statement_timeout(60000)  # 60 seconds
                
                # Total events older than cutoff
                cur.execute("""
                    SELECT COUNT(*) 
                    FROM events 
                    WHERE received_at < %s
                """, (cutoff_date,))
                total_old = cur.fetchone()[0]
                
                # Events referenced in function_runs
                cur.execute("""
                    SELECT COUNT(*) 
                    FROM events e
                    WHERE e.received_at < %s
                    AND EXISTS (
                        SELECT 1 FROM function_runs fr 
                        WHERE fr.event_id = e.internal_id
                    )
                """, (cutoff_date,))
                referenced_in_function_runs = cur.fetchone()[0]
                
                # Events referenced in event_batches
                cur.execute("""
                    SELECT COUNT(*) 
                    FROM events e
                    WHERE e.received_at < %s
                    AND EXISTS (
                        SELECT 1 FROM event_batches eb 
                        WHERE position(encode(e.internal_id, 'hex') in encode(eb.event_ids, 'hex')) > 0
                    )
                """, (cutoff_date,))
                referenced_in_batches = cur.fetchone()[0]
                
                # Events referenced in both
                cur.execute("""
                    SELECT COUNT(*) 
                    FROM events e
                    WHERE e.received_at < %s
                    AND EXISTS (
                        SELECT 1 FROM function_runs fr 
                        WHERE fr.event_id = e.internal_id
                    )
                    AND EXISTS (
                        SELECT 1 FROM event_batches eb 
                        WHERE position(encode(e.internal_id, 'hex') in encode(eb.event_ids, 'hex')) > 0
                    )
                """, (cutoff_date,))
                referenced_in_both = cur.fetchone()[0]
                
                # Events not referenced anywhere
                cur.execute("""
                    SELECT COUNT(*) 
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
                """, (cutoff_date,))
                not_referenced = cur.fetchone()[0]
                
                # Reset timeout
                self.set_statement_timeout()
        
        self.logger.info(f"Event Reference Analysis:")
        self.logger.info(f"  Total events older than cutoff: {total_old:,}")
        self.logger.info(f"  Referenced in function_runs: {referenced_in_function_runs:,} ({referenced_in_function_runs/total_old*100:.1f}%)")
        self.logger.info(f"  Referenced in event_batches: {referenced_in_batches:,} ({referenced_in_batches/total_old*100:.1f}%)")
        self.logger.info(f"  Referenced in both tables: {referenced_in_both:,} ({referenced_in_both/total_old*100:.1f}%)")
        self.logger.info(f"  Not referenced (eligible for deletion): {not_referenced:,} ({not_referenced/total_old*100:.1f}%)")
        
        return {
            'total_old': total_old,
            'referenced_in_function_runs': referenced_in_function_runs,
            'referenced_in_batches': referenced_in_batches,
            'referenced_in_both': referenced_in_both,
            'not_referenced': not_referenced
        }

    def get_primary_key_column(self, table_name):
        """Try to determine the primary key column for a table"""
        with self.db_retry():
            with self.conn.cursor() as cur:
                # First try to get the primary key information
                cur.execute("""
                    SELECT a.attname 
                    FROM pg_index i
                    JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
                    WHERE i.indrelid = %s::regclass
                    AND i.indisprimary
                """, (table_name,))
                rows = cur.fetchall()
                if rows:
                    return rows[0][0]  # First column of the primary key
                    
                # If that fails, look for common primary key column names
                columns = self.get_table_columns(table_name)
                for candidate in ['id', 'uid', 'uuid', 'pk', 'key', 'internal_id']:
                    if candidate in columns:
                        return candidate
                        
                # If all else fails, just return the first column
                if columns:
                    return columns[0]
                    
                return None

    def cleanup_function_runs(self):
        """Clean up old function runs that are older than the retention period"""
        self.logger.info(f"Starting function_runs cleanup with retention period of {self.retention_days} days")
        
        # First, let's determine the columns to use
        try:
            # Get the columns in the table
            columns = self.get_table_columns('function_runs')
            self.logger.debug(f"Available function_runs columns: {columns}")
            
            # Get the primary key column
            id_column = self.get_primary_key_column('function_runs')
            if not id_column:
                self.logger.error("Could not determine ID column for function_runs table")
                return 0
                
            self.logger.info(f"Using ID column '{id_column}' for function_runs cleanup")
            
            # We already know the timestamp column is run_started_at
            if 'run_started_at' not in columns:
                self.logger.error("run_started_at column not found in function_runs table")
                self.logger.error(f"Available columns: {columns}")
                return 0
        except Exception as e:
            self.logger.error(f"Error determining columns for function_runs: {e}")
            return 0
        
        cutoff_date = datetime.now(UTC) - timedelta(days=self.retention_days)
        total_deleted = 0
        start_time = time.time()
        no_more_runs = False
        
        # For dry run mode, we need to keep track of processed IDs
        processed_ids = set()
        batch_num = 0
        
        while not self.check_should_exit() and not no_more_runs:
            try:
                with self.db_retry():
                    # Set a reasonable timeout
                    self.set_statement_timeout(120000)  # 2 minutes
                    
                    with self.conn.cursor() as cur:
                        batch_num += 1
                        self.logger.info(f"Processing function_runs batch #{batch_num}")
                        
                        # First get a count for dry-run mode
                        if self.dry_run:
                            self.logger.info("Running function_runs dry run query...")
                            
                            # If we've already processed some IDs, exclude them
                            if processed_ids:
                                query = f"""
                                    SELECT {id_column} 
                                    FROM function_runs
                                    WHERE run_started_at < %s
                                    AND {id_column} NOT IN (SELECT unnest(%s))
                                    ORDER BY run_started_at
                                    LIMIT %s
                                """
                                cur.execute(query, (cutoff_date, list(processed_ids), self.batch_size))
                            else:
                                query = f"""
                                    SELECT {id_column} 
                                    FROM function_runs
                                    WHERE run_started_at < %s
                                    ORDER BY run_started_at
                                    LIMIT %s
                                """
                                cur.execute(query, (cutoff_date, self.batch_size))
                                
                            # Get the IDs and count
                            rows = cur.fetchall()
                            count = len(rows)
                            
                            # Add these IDs to our processed set
                            if count > 0:
                                new_ids = {row[0] for row in rows}
                                processed_ids.update(new_ids)
                            
                        else:
                            self.logger.info("Running function_runs delete query...")
                            query = f"""
                                WITH deleted AS (
                                    DELETE FROM function_runs
                                    WHERE run_started_at < %s
                                    ORDER BY run_started_at
                                    LIMIT %s
                                    RETURNING 1
                                )
                                SELECT COUNT(*) FROM deleted
                            """
                            cur.execute(query, (cutoff_date, self.batch_size))
                            count = cur.fetchone()[0]
                        
                        if not count:
                            self.logger.info("No more function_runs to clean up")
                            no_more_runs = True
                            break
                            
                        if self.dry_run:
                            self.logger.info(f"Would delete {count} function_runs (total processed so far: {len(processed_ids)})")
                        else:
                            total_deleted += count
                            elapsed = time.time() - start_time
                            rate = total_deleted / elapsed if elapsed > 0 else 0
                            self.logger.info(
                                f"Deleted {count} function_runs "
                                f"(Total: {total_deleted:,}, Rate: {rate:.1f} runs/sec)"
                            )
                        
                        # If we didn't get a full batch, we're likely done
                        if count < self.batch_size:
                            self.logger.info("Received less than a full batch, likely done")
                            no_more_runs = True
                            
                        if self.sleep_seconds > 0 and not self.check_should_exit() and not no_more_runs:
                            time.sleep(self.sleep_seconds)
            except Exception as e:
                if self.check_should_exit():
                    break
                self.logger.error(f"Error during function_runs cleanup batch: {e}")
                time.sleep(self.retry_delay)
        
        if total_deleted > 0 and not self.dry_run:
            elapsed = time.time() - start_time
            final_rate = total_deleted / elapsed if elapsed > 0 else 0
            self.logger.info(
                f"Function runs cleanup completed: {total_deleted:,} runs deleted in "
                f"{elapsed:.1f} seconds ({final_rate:.1f} runs/sec)"
            )
        elif self.dry_run:
            self.logger.info(f"Function runs cleanup (dry run) completed: would delete {len(processed_ids):,} runs")
            
        return total_deleted if not self.dry_run else len(processed_ids)

    def get_table_columns(self, table_name):
        """Get all column names for a given table"""
        with self.db_retry():
            with self.conn.cursor() as cur:
                cur.execute("""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_name = %s
                """, (table_name,))
                columns = [row[0] for row in cur.fetchall()]
                self.logger.debug(f"Table {table_name} columns: {columns}")
                return columns

    def cleanup_event_batches(self):
        """Clean up old event batches that are older than the retention period"""
        self.logger.info(f"Starting event_batches cleanup with retention period of {self.retention_days} days")
        
        # First, let's determine the timestamp column to use
        try:
            columns = self.get_table_columns('event_batches')
            
            # Try to find a timestamp column based on common naming patterns
            timestamp_column = None
            for candidate in ['created_at', 'timestamp', 'queued_at', 'sent_at', 'processed_at', 'completed_at']:
                if candidate in columns:
                    timestamp_column = candidate
                    break
                    
            if not timestamp_column:
                self.logger.error("Could not determine timestamp column for event_batches table")
                self.logger.error(f"Available columns: {columns}")
                return 0
                
            self.logger.info(f"Using timestamp column '{timestamp_column}' for event_batches cleanup")
        except Exception as e:
            self.logger.error(f"Error determining timestamp column: {e}")
            return 0
        
        cutoff_date = datetime.now(UTC) - timedelta(days=self.retention_days)
        total_deleted = 0
        start_time = time.time()
        no_more_batches = False
        
        # For dry run mode, we need to keep track of processed IDs
        processed_ids = set()
        batch_num = 0
        
        while not self.check_should_exit() and not no_more_batches:
            try:
                with self.db_retry():
                    # Set a reasonable timeout
                    self.set_statement_timeout(120000)  # 2 minutes
                    
                    with self.conn.cursor() as cur:
                        batch_num += 1
                        self.logger.info(f"Processing event_batches batch #{batch_num}")
                        
                        # First get a count for dry-run mode
                        if self.dry_run:
                            self.logger.info("Running event_batches dry run query...")
                            
                            # If we've already processed some IDs, exclude them
                            if processed_ids:
                                query = f"""
                                    SELECT id 
                                    FROM event_batches
                                    WHERE {timestamp_column} < %s
                                    AND id NOT IN (SELECT unnest(%s))
                                    ORDER BY {timestamp_column}
                                    LIMIT %s
                                """
                                cur.execute(query, (cutoff_date, list(processed_ids), self.batch_size))
                            else:
                                query = f"""
                                    SELECT id 
                                    FROM event_batches
                                    WHERE {timestamp_column} < %s
                                    ORDER BY {timestamp_column}
                                    LIMIT %s
                                """
                                cur.execute(query, (cutoff_date, self.batch_size))
                                
                            # Get the IDs and count
                            rows = cur.fetchall()
                            count = len(rows)
                            
                            # Add these IDs to our processed set
                            if count > 0:
                                new_ids = {row[0] for row in rows}
                                processed_ids.update(new_ids)
                                
                        else:
                            self.logger.info("Running event_batches delete query...")
                            query = f"""
                                WITH deleted AS (
                                    DELETE FROM event_batches
                                    WHERE {timestamp_column} < %s
                                    ORDER BY {timestamp_column}
                                    LIMIT %s
                                    RETURNING 1
                                )
                                SELECT COUNT(*) FROM deleted
                            """
                            cur.execute(query, (cutoff_date, self.batch_size))
                            count = cur.fetchone()[0]
                        
                        if not count:
                            self.logger.info("No more event_batches to clean up")
                            no_more_batches = True
                            break
                            
                        if self.dry_run:
                            self.logger.info(f"Would delete {count} event_batches (total processed so far: {len(processed_ids)})")
                        else:
                            total_deleted += count
                            elapsed = time.time() - start_time
                            rate = total_deleted / elapsed if elapsed > 0 else 0
                            self.logger.info(
                                f"Deleted {count} event_batches "
                                f"(Total: {total_deleted:,}, Rate: {rate:.1f} batches/sec)"
                            )
                        
                        # If we didn't get a full batch, we're likely done
                        if count < self.batch_size:
                            self.logger.info("Received less than a full batch, likely done")
                            no_more_batches = True
                            
                        if self.sleep_seconds > 0 and not self.check_should_exit() and not no_more_batches:
                            time.sleep(self.sleep_seconds)
            except Exception as e:
                if self.check_should_exit():
                    break
                self.logger.error(f"Error during event_batches cleanup batch: {e}")
                time.sleep(self.retry_delay)
        
        if total_deleted > 0 and not self.dry_run:
            elapsed = time.time() - start_time
            final_rate = total_deleted / elapsed if elapsed > 0 else 0
            self.logger.info(
                f"Event batches cleanup completed: {total_deleted:,} batches deleted in "
                f"{elapsed:.1f} seconds ({final_rate:.1f} batches/sec)"
            )
        elif self.dry_run:
            self.logger.info(f"Event batches cleanup (dry run) completed: would delete {len(processed_ids):,} batches")
            
        return total_deleted if not self.dry_run else len(processed_ids)

    def full_cleanup(self):
        """Perform a full cleanup of all tables in the correct order"""
        self.logger.info(f"Starting full cleanup with retention period of {self.retention_days} days")
        
        # First clean up function runs
        self.logger.info("Phase 1: Cleaning up function_runs")
        function_runs_deleted = self.cleanup_function_runs()
        
        if self.check_should_exit():
            return
            
        # Then clean up event batches
        self.logger.info("Phase 2: Cleaning up event_batches")
        event_batches_deleted = self.cleanup_event_batches()
        
        if self.check_should_exit():
            return
            
        # Finally clean up events
        self.logger.info("Phase 3: Cleaning up events")
        self.cleanup()
        
        self.logger.info("Full cleanup process completed")

    def inspect_schema(self):
        """Inspect the database schema for relevant tables"""
        self.logger.info("Inspecting database schema for relevant tables")
        tables_to_check = ['events', 'function_runs', 'event_batches']
        
        for table in tables_to_check:
            try:
                columns = self.get_table_columns(table)
                self.logger.info(f"Table '{table}' columns:")
                for col in columns:
                    self.logger.info(f"  - {col}")
                    
                # If this is the events table, also check the referenced tables
                if table == 'events':
                    try:
                        with self.db_retry():
                            with self.conn.cursor() as cur:
                                # Check how many rows are in each table
                                for check_table in tables_to_check:
                                    cur.execute(f"SELECT COUNT(*) FROM {check_table}")
                                    count = cur.fetchone()[0]
                                    self.logger.info(f"Table '{check_table}' has {count:,} rows")
                    except Exception as e:
                        self.logger.error(f"Error counting rows: {e}")
            except Exception as e:
                self.logger.error(f"Error inspecting schema for table '{table}': {e}")
                
        return True

def main():
    # Read configuration from environment variables with fallbacks to command-line arguments
    parser = argparse.ArgumentParser(description='Clean up old events from the database')
    parser.add_argument('--db-url', help='Database connection URL')
    parser.add_argument('--retention-days', type=int, default=3, help='Number of days to retain events')
    parser.add_argument('--batch-size', type=int, default=5000, help='Number of events to process in each batch')
    parser.add_argument('--sleep-seconds', type=float, default=0.1, help='Seconds to sleep between batches')
    parser.add_argument('--dry-run', action='store_true', help='Print what would be done without making changes')
    parser.add_argument('--max-retries', type=int, default=3, help='Maximum number of retries for database operations')
    parser.add_argument('--retry-delay', type=int, default=5, help='Delay in seconds between retries')
    parser.add_argument('--run-interval', type=int, default=0, help='Run every N minutes (0 = run once and exit)')
    parser.add_argument('--healthcheck-port', type=int, default=8080, help='Port for healthcheck server (0 = disabled)')
    parser.add_argument('--log-level', default='INFO', help='Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)')
    parser.add_argument('--debug-references', action='store_true', help='Run analysis of event references')
    parser.add_argument('--inspect-schema', action='store_true', help='Inspect database schema and exit')
    
    # Add cleanup strategy options
    cleanup_group = parser.add_argument_group('cleanup strategy')
    cleanup_group.add_argument('--clean-events-only', action='store_true', help='Only clean up events table')
    cleanup_group.add_argument('--clean-function-runs-only', action='store_true', help='Only clean up function_runs table')
    cleanup_group.add_argument('--clean-event-batches-only', action='store_true', help='Only clean up event_batches table')
    cleanup_group.add_argument('--full-cleanup', action='store_true', help='Clean up all tables in the proper order (default)')

    args = parser.parse_args()
    
    # Support for environment variables (higher priority than command line args)
    db_url = os.environ.get('DB_URL', args.db_url)
    if not db_url:
        print("Error: DB_URL must be provided via environment variable or --db-url argument")
        sys.exit(1)
    
    # Safely parse integer/float environment variables
    def safe_int(env_var, default_value):
        try:
            return int(os.environ.get(env_var, default_value))
        except (ValueError, TypeError):
            print(f"Warning: Invalid value for {env_var}, using default: {default_value}")
            return default_value
    
    def safe_float(env_var, default_value):
        try:
            return float(os.environ.get(env_var, default_value))
        except (ValueError, TypeError):
            print(f"Warning: Invalid value for {env_var}, using default: {default_value}")
            return default_value
            
    retention_days = safe_int('RETENTION_DAYS', args.retention_days)
    batch_size = safe_int('BATCH_SIZE', args.batch_size)
    sleep_seconds = safe_float('SLEEP_SECONDS', args.sleep_seconds)
    dry_run = os.environ.get('DRY_RUN', '').lower() in ('true', 't', 'yes', 'y', '1') or args.dry_run
    max_retries = safe_int('MAX_RETRIES', args.max_retries)
    retry_delay = safe_int('RETRY_DELAY', args.retry_delay)
    run_interval = safe_int('RUN_INTERVAL', args.run_interval)
    healthcheck_port = safe_int('HEALTHCHECK_PORT', args.healthcheck_port)
    log_level = os.environ.get('LOG_LEVEL', args.log_level).upper()
    debug_references = os.environ.get('DEBUG_REFERENCES', '').lower() in ('true', 't', 'yes', 'y', '1') or args.debug_references
    inspect_schema = os.environ.get('INSPECT_SCHEMA', '').lower() in ('true', 't', 'yes', 'y', '1') or args.inspect_schema
    
    # Parse cleanup strategy from environment variables
    clean_events_only = os.environ.get('CLEAN_EVENTS_ONLY', '').lower() in ('true', 't', 'yes', 'y', '1') or args.clean_events_only
    clean_function_runs_only = os.environ.get('CLEAN_FUNCTION_RUNS_ONLY', '').lower() in ('true', 't', 'yes', 'y', '1') or args.clean_function_runs_only
    clean_event_batches_only = os.environ.get('CLEAN_EVENT_BATCHES_ONLY', '').lower() in ('true', 't', 'yes', 'y', '1') or args.clean_event_batches_only
    full_cleanup = os.environ.get('FULL_CLEANUP', '').lower() in ('true', 't', 'yes', 'y', '1') or args.full_cleanup
    
    # If no specific cleanup strategy is specified, default to full cleanup
    if not any([clean_events_only, clean_function_runs_only, clean_event_batches_only, full_cleanup]):
        full_cleanup = True

    # Set up logging at the top level for interval messages
    logger = logging.getLogger('event_cleaner')
    
    # Set log level based on configuration
    valid_levels = ('DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL')
    if log_level not in valid_levels:
        print(f"Warning: Invalid log level '{log_level}', using INFO")
        log_level = 'INFO'
        
    log_level_obj = getattr(logging, log_level)
    logger.setLevel(log_level_obj)
    
    # Check if the logger already has handlers to avoid duplicates
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(
            logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        )
        logger.addHandler(handler)

    # Set up signal handler at the top level for graceful shutdown
    should_exit = False
    
    def signal_handler(signum, frame):
        nonlocal should_exit
        logger.info(f"Received signal {signum}. Initiating graceful shutdown...")
        should_exit = True
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Create a callback function to check exit status
    def should_exit_check():
        return should_exit

    # Health status function for healthcheck server
    health_server = None
    if healthcheck_port > 0:
        def health_status_func():
            # Only report healthy if:
            # 1. The service isn't exiting
            # 2. The database connection is healthy
            global db_is_healthy
            with db_health_lock:
                is_healthy = not should_exit and db_is_healthy
            return is_healthy
            
        try:
            health_server = start_health_server(
                healthcheck_port,
                health_status_func,
                logger
            )
        except Exception as e:
            logger.error(f"Failed to start health check server: {e}")
    
    cleaner = None
    try:
        while not should_exit:
            start_time = time.time()
            
            try:
                cleaner = EventCleaner(
                    db_url=db_url,
                    retention_days=retention_days,
                    batch_size=batch_size,
                    sleep_seconds=sleep_seconds,
                    dry_run=dry_run,
                    max_retries=max_retries,
                    retry_delay=retry_delay,
                    logger=logger,
                    should_exit_callback=should_exit_check
                )

                # Run reference analysis if requested
                if debug_references:
                    logger.info("Running event reference analysis")
                    cleaner.debug_event_references()
                
                # Inspect schema if requested
                if inspect_schema:
                    logger.info("Inspecting database schema")
                    cleaner.inspect_schema()
                    continue  # Skip to next interval without cleanup
                
                # Run the appropriate cleanup strategy
                if full_cleanup:
                    cleaner.full_cleanup()
                elif clean_events_only:
                    cleaner.cleanup()
                elif clean_function_runs_only:
                    cleaner.cleanup_function_runs()
                elif clean_event_batches_only:
                    cleaner.cleanup_event_batches()
                else:
                    # This shouldn't happen due to our default setting above
                    logger.warning("No cleanup strategy specified, defaulting to full cleanup")
                    cleaner.full_cleanup()
            except Exception as e:
                logger.error(f"Error during cleanup: {e}")
                time.sleep(retry_delay)  # Wait before retrying
            finally:
                if cleaner:
                    try:
                        cleaner.close()
                    except Exception as e:
                        logger.error(f"Error closing cleaner: {e}")
                    cleaner = None
                
            # If run-interval is 0 or we need to exit, break out of the loop
            if run_interval == 0 or should_exit:
                break
                
            # Calculate the time to sleep until the next run
            elapsed_time = time.time() - start_time
            sleep_time = max(0, run_interval * 60 - elapsed_time)
            
            if sleep_time > 0 and not should_exit:
                logger.info(f"Cleanup completed. Next run in {sleep_time:.1f} seconds")
                
                # Sleep in small increments to check for exit signal
                while sleep_time > 0 and not should_exit:
                    increment = min(1.0, sleep_time)
                    time.sleep(increment)
                    sleep_time -= increment
    
    except Exception as e:
        logger.error(f"Unhandled exception: {e}")
        sys.exit(1)
    finally:
        # Ensure cleaner is closed if it exists
        if cleaner:
            try:
                cleaner.close()
            except Exception:
                pass
                
        # Shut down health check server if it's running
        if health_server:
            try:
                logger.info("Shutting down health check server")
                health_server.shutdown()
            except Exception as e:
                logger.error(f"Error shutting down health check server: {e}")
    
    logger.info("Exiting cleanup process")

if __name__ == '__main__':
    main()
