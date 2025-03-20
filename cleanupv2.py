#!/usr/bin/env python3
import psycopg2
import time
import logging
from datetime import datetime, timedelta, UTC
import os
# Configure logging for detailed output
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

BATCH_SIZE = 10000  # Adjust the batch size as needed

def get_db_connection():
    # Replace with your actual database connection parameters
    postgres_uri = os.environ["POSTGRES_URI"]
    return psycopg2.connect(postgres_uri)

def create_indexes(conn):
    indexes = [
        ("idx_function_finishes_created_at", "CREATE INDEX IF NOT EXISTS idx_function_finishes_created_at ON public.function_finishes(created_at)"),
        ("idx_function_runs_original_run_id", "CREATE INDEX IF NOT EXISTS idx_function_runs_original_run_id ON public.function_runs(original_run_id)"),
        ("idx_history_run_id", "CREATE INDEX IF NOT EXISTS idx_history_run_id ON public.history(run_id)"),
        ("idx_traces_run_id", "CREATE INDEX IF NOT EXISTS idx_traces_run_id ON public.traces(run_id)"),
        ("idx_trace_runs_ended_at", "CREATE INDEX IF NOT EXISTS idx_trace_runs_ended_at ON public.trace_runs(ended_at)"),
        ("idx_events_received_at", "CREATE INDEX IF NOT EXISTS idx_events_received_at ON public.events(received_at)"),
        ("idx_events_internal_id", "CREATE INDEX IF NOT EXISTS idx_events_internal_id ON public.events(internal_id)"),
        ("idx_function_runs_event_id", "CREATE INDEX IF NOT EXISTS idx_function_runs_event_id ON public.function_runs(event_id)"),
        ("idx_events_event_id", "CREATE INDEX IF NOT EXISTS idx_events_event_id ON public.events(event_id)"),
        ("idx_history_event_id", "CREATE INDEX IF NOT EXISTS idx_history_event_id ON public.history(event_id)"),

        # Index for event_batches to improve the POSITION lookup
        # ("idx_event_batches_event_ids", "CREATEx INDEX IF NOT EXISTS idx_event_batches_event_ids ON public.event_batches USING gin (event_ids)")
        
        # Add this index which is critical for the query
    ]
    
    with conn.cursor() as cur:
        for index_name, index_sql in indexes:
            try:
                logging.info("Creating index if not exists: %s", index_name)
                cur.execute(index_sql)
                conn.commit()
            except psycopg2.errors.DuplicateTable:
                conn.rollback()
                logging.info("Index %s already exists.", index_name)
            except Exception as e:
                conn.rollback()
                logging.warning("Failed to create index %s: %s", index_name, str(e))
    logging.info("Index creation complete.")

def cleanup_function_data(conn, cutoff, batch_size=BATCH_SIZE):
    """
    Cleanup function-related data (function_finishes, function_runs, history).
    Returns the number of candidate run_ids processed.
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT ff.run_id
            FROM public.function_finishes ff
            WHERE ff.created_at < %s
              AND NOT EXISTS (
                  SELECT 1
                  FROM public.function_runs fr
                  LEFT JOIN public.function_finishes ff2
                    ON fr.run_id = ff2.run_id
                  WHERE fr.original_run_id = ff.run_id
                    AND (ff2.run_id IS NULL OR ff2.created_at >= %s)
              )
            ORDER BY ff.created_at ASC
            LIMIT %s
        """, (cutoff, cutoff, batch_size))
        rows = cur.fetchall()
        run_ids = [row[0] for row in rows]

    if not run_ids:
        return 0

    with conn.cursor() as cur:
        cur.execute("DELETE FROM public.function_runs WHERE run_id = ANY(%s)", (run_ids,))
        cur.execute("DELETE FROM public.function_finishes WHERE run_id = ANY(%s)", (run_ids,))
        cur.execute("DELETE FROM public.history WHERE run_id = ANY(%s)", (run_ids,))
        conn.commit()

    logging.info("Processed %d function-data rows.", len(run_ids))
    return len(run_ids)

def cleanup_events(conn, cutoff, batch_size=BATCH_SIZE):
    """
    Cleanup events with an inverted approach:
    1. Find the (smaller) set of event IDs that ARE referenced
    2. Delete everything else that's older than the cutoff
    
    This works better when most events are orphaned.
    """
    logging.info("Cleaning up events before %s", cutoff.strftime("%Y-%m-%d %H:%M:%S"))
    
    with conn.cursor() as cur:
        # Create a temporary table of VALID event IDs (the smaller set)
        cur.execute("""
            CREATE TEMPORARY TABLE keep_events AS
            SELECT DISTINCT event_id FROM (
                SELECT event_id FROM public.function_runs
                UNION ALL
                SELECT event_id FROM public.history
            ) AS referenced_ids;
            
            CREATE INDEX ON keep_events(event_id);
        """)
        
        # Now delete events that are both old AND not in our keep list
        # Using row_ctid as an alias for the system column ctid
        cur.execute("""
            CREATE TEMPORARY TABLE delete_ids AS
            SELECT e.ctid AS row_ctid
            FROM public.events e
            WHERE e.received_at < %s
            AND NOT EXISTS (
                SELECT 1 FROM keep_events k 
                WHERE k.event_id = e.internal_id
            )
            LIMIT %s;
        """, (cutoff, batch_size))
        
        # Delete using the ctids (most efficient way)
        cur.execute("""
            DELETE FROM public.events
            WHERE ctid IN (SELECT row_ctid FROM delete_ids);
        """)
        
        deleted = cur.rowcount
        
        # Clean up temporary tables
        cur.execute("DROP TABLE IF EXISTS keep_events;")
        cur.execute("DROP TABLE IF EXISTS delete_ids;")
        conn.commit()
    
    logging.info("Processed %d orphaned event rows.", deleted)
    return deleted

def cleanup_trace_data(conn, cutoff, batch_size=BATCH_SIZE):
    """
    Cleanup trace_runs and associated traces where ended_at (converted to timestamp) is older than the cutoff.
    Returns the number of trace_run rows processed.
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT run_id
            FROM public.trace_runs
            WHERE ended_at IS NOT NULL
              AND to_timestamp(ended_at) < %s
            ORDER BY ended_at ASC
            LIMIT %s
        """, (cutoff, batch_size))
        rows = cur.fetchall()
        trace_run_ids = [row[0] for row in rows]

    if not trace_run_ids:
        return 0

    with conn.cursor() as cur:
        cur.execute("DELETE FROM public.traces WHERE run_id = ANY(%s)", (trace_run_ids,))
        cur.execute("DELETE FROM public.trace_runs WHERE run_id = ANY(%s)", (trace_run_ids,))
        conn.commit()

    logging.info("Processed %d trace-data rows.", len(trace_run_ids))
    return len(trace_run_ids)

def run_maintenance( perform_vacuum=True, perform_reindex=True):
    """
    Run maintenance tasks on cleaned tables:
      - VACUUM ANALYZE to reclaim space and update statistics.
      - REINDEX indexes concurrently to rebuild indexes with minimal locking.
    
    Note: For VACUUM operations we need a separate connection with autocommit enabled.
    """
    logging.info("Starting maintenance tasks.")
    
    # Create a new connection in autocommit mode for VACUUM operations
    maintenance_conn = get_db_connection()
    maintenance_conn.autocommit = True
    
    maintenance_tables = [
        "public.function_runs",
        "public.function_finishes",
        "public.history",
        "public.events",
        "public.trace_runs",
        "public.traces"
    ]
    
    try:
        if perform_vacuum:
            with maintenance_conn.cursor() as cur:
                for table in maintenance_tables:
                    logging.info("Vacuuming and analyzing table: %s", table)
                    cur.execute(f"VACUUM ANALYZE {table};")
                
        # List of indexes to reindex concurrently
        indexes = [
            "idx_function_finishes_created_at",
            "idx_function_runs_original_run_id",
            "idx_history_run_id",
            "idx_traces_run_id",
            "idx_trace_runs_ended_at",
            "idx_events_received_at",
            "idx_events_internal_id",
            "idx_function_runs_event_id",
            "idx_events_event_id",
            "idx_history_event_id",
            # "idx_event_batches_event_ids" # Add when deleting by batch.
        ]
        if perform_reindex:
            with maintenance_conn.cursor() as cur:
                for index in indexes:
                    logging.info("Reindexing index concurrently: %s", index)
                    cur.execute(f"REINDEX INDEX CONCURRENTLY {index};")
                
        logging.info("Maintenance complete.")
    except Exception as e:
        logging.exception("Error during maintenance: %s", e)
    finally:
        maintenance_conn.close()
        logging.info("Maintenance connection closed.")

def diagnose_query_performance(conn, query, params=None):
    """
    Run EXPLAIN ANALYZE on a query to diagnose performance issues.
    """
    logging.info("Diagnosing query performance...")
    explain_query = f"EXPLAIN ANALYZE {query}"
    
    with conn.cursor() as cur:
        cur.execute(explain_query, params or ())
        plan = cur.fetchall()
        
    for line in plan:
        logging.info(line[0])
    
    return plan

def main():
    logging.info("Starting batch cleanup process.")
    conn = get_db_connection()
    try:
        cutoff = datetime.utcnow() - timedelta(hours=11)
        create_indexes(conn)

        # Setup state tracking for each cleanup operation.
        tasks_state = {
            "function_data": True,
            "events": True,
            "trace_data": True,
        }

        while any(tasks_state.values()):
            if tasks_state["function_data"]:
                count = cleanup_function_data(conn, cutoff)
                if count < BATCH_SIZE:
                    tasks_state["function_data"] = False
                    logging.info("Function data cleanup complete.")
                time.sleep(0.2)

            if tasks_state["events"]:
                count = cleanup_events(conn, cutoff)
                if count < BATCH_SIZE:
                    tasks_state["events"] = False
                    logging.info("Orphan events cleanup complete.")
                time.sleep(0.2)

            if tasks_state["trace_data"]:
                count = cleanup_trace_data(conn, cutoff)
                if count < BATCH_SIZE:
                    tasks_state["trace_data"] = False
                    logging.info("Trace data cleanup complete.")
                time.sleep(0.2)

            logging.info("Current task state: %s", tasks_state)
            time.sleep(0.4)
            i+=1

        logging.info("All cleanup operations complete.")
        # Run maintenance in a separate connection with autocommit=True
        # Check if current time in Phoenix (MST/UTC-7) is between 10pm and 7am
        # Convert current UTC time to Phoenix time
        current_utc = datetime.now(UTC)
        phoenix_offset = -7  # Phoenix is UTC-7 (MST)
        phoenix_time = current_utc + timedelta(hours=phoenix_offset)
        
        # Check if time is between 10pm (22:00) and 7am (07:00)
        phoenix_hour = phoenix_time.hour
        maintenance_window = phoenix_hour >= 22 or phoenix_hour < 7
        run_maintenance(perform_vacuum=True, perform_reindex=maintenance_window)

    except Exception as e:
        logging.exception("Error during cleanup: %s", e)
        conn.rollback()
    finally:
        conn.close()
        logging.info("Database connection closed.")

if __name__ == "__main__":
    main()