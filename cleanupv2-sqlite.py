#!/usr/bin/env python3
import sqlite3
import time
import logging
from datetime import datetime, timedelta, UTC
import os

# Configure logging for detailed output
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# Adjust the batch size as needed
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "6000"))
# Path to the SQLite database file
SQLITE_DB_PATH = os.environ.get("SQLITE_DB_PATH", "/inngest_sqlite_data/main.db")
# Retention period in hours
CLEANUP_RETENTION_HOURS = int(os.environ.get("CLEANUP_RETENTION_HOURS", f"{30*24}")) # Default: 30 days

def get_db_connection():
    """Establishes a connection to the SQLite database."""
    try:
        # isolation_level=None enables autocommit mode for potential maintenance tasks if needed separately
        # However, standard operations will use transactions explicitly or via 'with connection:'
        conn = sqlite3.connect(SQLITE_DB_PATH, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
        # Enable foreign key constraints if they are defined in the schema
        conn.execute("PRAGMA foreign_keys = ON;")
        logging.info("Successfully connected to SQLite database: %s", SQLITE_DB_PATH)
        return conn
    except sqlite3.Error as e:
        logging.exception("Error connecting to SQLite database: %s", e)
        raise

def adapt_identifier(name):
    """Removes 'public.' prefix if present, as it's not typically used in SQLite."""
    return name.replace("public.", "")

def create_indexes(conn):
    """Creates necessary indexes if they don't exist."""
    # Note: HASH index type is PG specific, use standard BTREE for SQLite
    # Note: GIN index type is PG specific, not directly applicable for event_ids array in SQLite
    indexes = [
        ("idx_function_finishes_created_at", "CREATE INDEX IF NOT EXISTS idx_function_finishes_created_at ON function_finishes(created_at)"),
        ("idx_function_runs_original_run_id", "CREATE INDEX IF NOT EXISTS idx_function_runs_original_run_id ON function_runs(original_run_id)"),
        ("idx_history_run_id", "CREATE INDEX IF NOT EXISTS idx_history_run_id ON history(run_id)"),
        ("idx_traces_run_id", "CREATE INDEX IF NOT EXISTS idx_traces_run_id ON traces(run_id)"),
        ("idx_trace_runs_ended_at", "CREATE INDEX IF NOT EXISTS idx_trace_runs_ended_at ON trace_runs(ended_at)"),
        ("idx_events_received_at", "CREATE INDEX IF NOT EXISTS idx_events_received_at ON events(received_at)"),
        ("idx_events_internal_id", "CREATE INDEX IF NOT EXISTS idx_events_internal_id ON events(internal_id)"), # Replaced HASH index
        ("idx_function_runs_event_id", "CREATE INDEX IF NOT EXISTS idx_function_runs_event_id ON function_runs(event_id)"),
        ("idx_events_event_id", "CREATE INDEX IF NOT EXISTS idx_events_event_id ON events(event_id)"),
        ("idx_history_event_id", "CREATE INDEX IF NOT EXISTS idx_history_event_id ON history(event_id)"),
        # GIN index for event_batches.event_ids is complex in SQLite. If needed,
        # the table schema or query approach might need adjustment (e.g., normalization).
    ]

    with conn: # Use 'with conn' for automatic transaction handling (commit/rollback)
        cur = conn.cursor()
        for index_name, index_sql in indexes:
            try:
                logging.info("Creating index if not exists: %s", index_name)
                cur.execute(adapt_identifier(index_sql)) # Adapt table names if needed
            except sqlite3.Error as e:
                # Basic error handling, might need refinement based on specific errors
                logging.warning("Failed to create index %s (might already exist or other issue): %s", index_name, str(e))
    logging.info("Index creation check complete.")


def cleanup_function_data(conn, cutoff_iso, batch_size=BATCH_SIZE):
    """
    Cleanup function-related data (function_finishes, function_runs, history).
    Returns the number of candidate run_ids processed.
    """
    run_ids = []
    # Use ISO format string for timestamp comparison in SQLite assuming TEXT storage
    # Adjust format or use Unix timestamp if stored differently
    find_sql = """
        SELECT ff.run_id
        FROM function_finishes ff
        WHERE ff.created_at < ?
          AND NOT EXISTS (
              SELECT 1
              FROM function_runs fr
              LEFT JOIN function_finishes ff2
                ON fr.run_id = ff2.run_id
              WHERE fr.original_run_id = ff.run_id
                AND (ff2.run_id IS NULL OR ff2.created_at >= ?)
          )
        ORDER BY ff.created_at ASC
        LIMIT ?
    """
    try:
        with conn:
            cur = conn.cursor()
            cur.execute(adapt_identifier(find_sql), (cutoff_iso, cutoff_iso, batch_size))
            rows = cur.fetchall()
            run_ids = [row[0] for row in rows]
    except sqlite3.Error as e:
        logging.error("Error selecting function data for deletion: %s", e)
        return 0 # Don't proceed if selection fails

    if not run_ids:
        return 0

    placeholders = ', '.join('?' * len(run_ids))
    try:
        with conn: # New transaction for delete operations
            cur = conn.cursor()
            cur.execute(adapt_identifier(f"DELETE FROM public.function_runs WHERE run_id IN ({placeholders})"), run_ids)
            cur.execute(adapt_identifier(f"DELETE FROM public.function_finishes WHERE run_id IN ({placeholders})"), run_ids)
            cur.execute(adapt_identifier(f"DELETE FROM public.history WHERE run_id IN ({placeholders})"), run_ids)
            # Commit happens automatically on exiting 'with' block successfully
    except sqlite3.Error as e:
        logging.error("Error deleting function data: %s", e)
        return 0 # Return 0 if deletion fails

    logging.info("Processed %d function-data rows.", len(run_ids))
    return len(run_ids)

def cleanup_events(conn, cutoff_iso, batch_size=BATCH_SIZE):
    """
    Cleanup orphaned events older than the cutoff using temporary tables and rowid.
    """
    logging.info("Cleaning up events before %s", cutoff_iso)
    deleted = 0
    try:
        with conn: # Use a single transaction for temp tables and deletion
            cur = conn.cursor()

            # Drop tables if they somehow persisted from a previous failed run
            cur.execute("DROP TABLE IF EXISTS temp_keep_events;")
            cur.execute("DROP TABLE IF EXISTS temp_delete_event_ids;")

            # Create a temporary table of VALID event IDs (referenced ones)
            # Ensure column names don't clash if tables have different structures
            cur.execute("""
                CREATE TEMPORARY TABLE temp_keep_events AS
                SELECT DISTINCT event_id FROM (
                    SELECT event_id FROM function_runs WHERE event_id IS NOT NULL
                    UNION -- Use UNION to implicitly get distinct IDs
                    SELECT event_id FROM history WHERE event_id IS NOT NULL
                ) AS referenced_ids;
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_temp_keep_events ON temp_keep_events(event_id);")

            # Find rowids of events to delete: old AND not referenced
            # Use ISO format string for cutoff comparison
            cur.execute(f"""
                CREATE TEMPORARY TABLE temp_delete_event_ids AS
                SELECT e.rowid AS row_id_to_delete
                FROM events e
                WHERE e.received_at < ?
                AND NOT EXISTS (
                    SELECT 1 FROM temp_keep_events k
                    WHERE k.event_id = e.internal_id -- Make sure internal_id is the correct join key
                )
                LIMIT ?;
            """, (cutoff_iso, batch_size))

            # Delete using the rowids (efficient in SQLite)
            cur.execute("""
                DELETE FROM events
                WHERE rowid IN (SELECT row_id_to_delete FROM temp_delete_event_ids);
            """)
            deleted = cur.rowcount

            # Temporary tables are automatically dropped at the end of the session/connection
            # Explicit drop within transaction ensures cleanup even if script continues
            cur.execute("DROP TABLE temp_keep_events;")
            cur.execute("DROP TABLE temp_delete_event_ids;")

    except sqlite3.Error as e:
        logging.error("Error during events cleanup: %s", e)
        # Transaction is automatically rolled back by 'with conn:' on error
        return 0

    logging.info("Processed %d orphaned event rows.", deleted)
    return deleted


def cleanup_trace_data(conn, cutoff_ms, batch_size=BATCH_SIZE):
    """
    Cleanup trace_runs and associated traces where ended_at (in milliseconds) is older than the cutoff.
    Returns the number of trace_run rows processed.
    """
    trace_run_ids = []
    # Assuming ended_at is stored as INTEGER in milliseconds
    find_sql = """
        SELECT run_id
        FROM trace_runs
        WHERE ended_at IS NOT NULL
          AND ended_at < ?
        ORDER BY ended_at ASC
        LIMIT ?
    """
    try:
        with conn:
            cur = conn.cursor()
            cur.execute(adapt_identifier(find_sql), (cutoff_ms, batch_size))
            rows = cur.fetchall()
            trace_run_ids = [row[0] for row in rows]
    except sqlite3.Error as e:
        logging.error("Error selecting trace data for deletion: %s", e)
        return 0

    if not trace_run_ids:
        return 0

    placeholders = ', '.join('?' * len(trace_run_ids))
    try:
        with conn:
            cur = conn.cursor()
            cur.execute(adapt_identifier(f"DELETE FROM public.traces WHERE run_id IN ({placeholders})"), trace_run_ids)
            cur.execute(adapt_identifier(f"DELETE FROM public.trace_runs WHERE run_id IN ({placeholders})"), trace_run_ids)
    except sqlite3.Error as e:
        logging.error("Error deleting trace data: %s", e)
        return 0

    logging.info("Processed %d trace-data rows.", len(trace_run_ids))
    return len(trace_run_ids)

def run_maintenance(perform_vacuum=True, perform_reindex=True):
    """
    Run maintenance tasks on the SQLite database:
      - VACUUM to reclaim space and rebuild the database file.
      - ANALYZE to update statistics for the query planner.
      - REINDEX to rebuild indexes.

    Note: These operations can lock the database. Run during off-peak hours.
    """
    logging.info("Starting maintenance tasks (VACUUM=%s, REINDEX=%s).", perform_vacuum, perform_reindex)
    # Use a separate connection for maintenance to manage transactions potentially differently
    # Or ensure the main connection commits before running these if not using autocommit.
    try:
        # Re-establish connection - simplest way to ensure no pending transaction
        maint_conn = get_db_connection()
        maint_conn.execute("PRAGMA journal_mode=WAL;") # Ensure WAL mode for better concurrency generally

        if perform_vacuum:
            logging.info("Running VACUUM...")
            start_time = time.time()
            maint_conn.execute("VACUUM;")
            logging.info("VACUUM complete (took %.2f seconds).", time.time() - start_time)

            logging.info("Running ANALYZE...")
            start_time = time.time()
            maint_conn.execute("ANALYZE;") # Analyze all tables/indexes
            logging.info("ANALYZE complete (took %.2f seconds).", time.time() - start_time)

        if perform_reindex:
            logging.info("Running REINDEX...")
            # Reindexing specific indexes can be done, but REINDEX without arguments rebuilds all.
            # This might be simpler unless specific index corruption is suspected.
            # List indexes if needed: SELECT name FROM sqlite_master WHERE type='index';
            start_time = time.time()
            try:
                # Example: Reindex a specific index known to exist
                # maint_conn.execute("REINDEX idx_events_received_at;")
                # Or reindex all indexes of a table:
                # maint_conn.execute("REINDEX events;")
                # Or reindex everything (simpler unless very large DB):
                 maint_conn.execute("REINDEX;") # Rebuilds all indexes for all tables
                 logging.info("REINDEX complete (took %.2f seconds).", time.time() - start_time)
            except sqlite3.Error as e:
                 logging.warning("Could not perform REINDEX: %s", e)


        maint_conn.close()
        logging.info("Maintenance connection closed.")

    except sqlite3.Error as e:
        logging.exception("Error during maintenance: %s", e)
    finally:
        # Ensure connection is closed if it exists and an error occurred before close
        if 'maint_conn' in locals() and maint_conn:
            try:
                maint_conn.close()
            except sqlite3.Error:
                pass # Ignore close errors if already failed

def main():
    logging.info("Starting batch cleanup process for SQLite DB: %s", SQLITE_DB_PATH)
    conn = None # Initialize conn to None
    try:
        conn = get_db_connection()
        cutoff_dt = datetime.now(UTC) - timedelta(hours=CLEANUP_RETENTION_HOURS)
        # Use ISO 8601 format string for comparisons, common for SQLite TEXT date storage
        cutoff_iso = cutoff_dt.isoformat()
        # Use Unix timestamp in milliseconds for trace data if stored as INTEGER
        cutoff_ms = int(cutoff_dt.timestamp() * 1000)

        logging.info("Cleanup cutoff time (UTC): %s", cutoff_iso)
        logging.info("Trace data cutoff time (ms): %d", cutoff_ms)

        # Ensure indexes exist before starting cleanup
        create_indexes(conn)

        tasks_state = {
            "function_data": True,
            "events": True,
            "trace_data": True,
        }
        total_processed = {"function_data": 0, "events": 0, "trace_data": 0}

        # Cleanup Loop
        while any(tasks_state.values()):
            if tasks_state["function_data"]:
                count = cleanup_function_data(conn, cutoff_iso)
                total_processed["function_data"] += count
                if count < BATCH_SIZE:
                    tasks_state["function_data"] = False
                    logging.info("Function data cleanup potentially complete.")
                time.sleep(0.2)

            if tasks_state["events"]:
                count = cleanup_events(conn, cutoff_iso)
                total_processed["events"] += count
                if count < BATCH_SIZE:
                    tasks_state["events"] = False
                    logging.info("Orphan events cleanup potentially complete.")
                time.sleep(0.2)

            if tasks_state["trace_data"]:
                count = cleanup_trace_data(conn, cutoff_ms)
                total_processed["trace_data"] += count
                if count < BATCH_SIZE:
                    tasks_state["trace_data"] = False
                    logging.info("Trace data cleanup potentially complete.")
                time.sleep(0.2)

            logging.debug("Current task state: %s", tasks_state)
            time.sleep(0.4) # Main loop sleep

        logging.info("All cleanup operations potentially complete.")
        logging.info("Total processed counts: %s", total_processed)

        # Maintenance Window Check (Example: Run daily around 4 AM MST/PDT)
        # This logic remains the same as it's Python-based time checking
        current_utc = datetime.now(UTC)
        # Arizona is MST (UTC-7) and does not observe DST
        arizona_offset = -7
        arizona_time = current_utc + timedelta(hours=arizona_offset)
        arizona_hour = arizona_time.hour

        # Run maintenance, e.g., between 3 AM and 5 AM Arizona time
        # Adjust perform_reindex=True/False as needed. Reindexing takes time/locks.
        if 3 <= arizona_hour < 5:
             logging.info("Within maintenance window (3-5 AM MST/PDT). Running maintenance.")
             # Close the main connection before starting maintenance if not using separate connections
             if conn:
                 conn.close()
                 conn = None # Ensure it's not used after closing
                 logging.info("Closed main DB connection before maintenance.")
             run_maintenance(perform_vacuum=True, perform_reindex=False) # Example: Skip REINDEX daily
        else:
             logging.info("Outside maintenance window (3-5 AM MST/PDT). Skipping maintenance.")

    except Exception as e:
        logging.exception("Fatal error during cleanup process: %s", e)
        # No explicit rollback needed if using 'with conn:' which handles it
    finally:
        if conn:
            conn.close()
            logging.info("Main database connection closed.")

if __name__ == "__main__":
    if not os.environ.get("SQLITE_DB_PATH"):
        logging.error("FATAL: SQLITE_DB_PATH environment variable not set.")
    else:
        main()