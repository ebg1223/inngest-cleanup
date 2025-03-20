#!/usr/bin/env python3
import psycopg2
import time
import logging
from datetime import datetime, timedelta
import os
# Configure logging for detailed output
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

BATCH_SIZE = 10000  # Adjust the batch size as needed

def get_db_connection():
    # Replace with your actual database connection parameters
    postgres_uri = os.environ["POSTGRES_URI"]
    return psycopg2.connect(postgres_uri)

def create_indexes(conn):
    """
    Ensure indexes exist for efficient cleanup.
    Existing indexes:
      - idx_function_finishes_created_at on function_finishes(created_at)
      - idx_function_runs_original_run_id on function_runs(original_run_id)
      - idx_history_run_id on history(run_id)
      - idx_traces_run_id on traces(run_id)
      - idx_trace_runs_ended_at on trace_runs(ended_at)
    Additional indexes:
      - idx_events_received_at on events(received_at)
      - idx_events_internal_id on events(internal_id)
      - idx_function_runs_event_id on function_runs(event_id)
    """
    indexes = [
        ("idx_function_finishes_created_at", "CREATE INDEX IF NOT EXISTS idx_function_finishes_created_at ON public.function_finishes(created_at)"),
        ("idx_function_runs_original_run_id", "CREATE INDEX IF NOT EXISTS idx_function_runs_original_run_id ON public.function_runs(original_run_id)"),
        ("idx_history_run_id", "CREATE INDEX IF NOT EXISTS idx_history_run_id ON public.history(run_id)"),
        ("idx_traces_run_id", "CREATE INDEX IF NOT EXISTS idx_traces_run_id ON public.traces(run_id)"),
        ("idx_trace_runs_ended_at", "CREATE INDEX IF NOT EXISTS idx_trace_runs_ended_at ON public.trace_runs(ended_at)"),
        ("idx_events_received_at", "CREATE INDEX IF NOT EXISTS idx_events_received_at ON public.events(received_at)"),
        ("idx_events_internal_id", "CREATE INDEX IF NOT EXISTS idx_events_internal_id ON public.events(internal_id)"),
        ("idx_function_runs_event_id", "CREATE INDEX IF NOT EXISTS idx_function_runs_event_id ON public.function_runs(event_id)")
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
    Cleanup orphaned events (i.e. events not referenced by any function_run).
    Returns the number of events deleted.
    """
    with conn.cursor() as cur:
        cur.execute("""
            WITH del AS (
                SELECT ctid
                FROM public.events e
                WHERE NOT EXISTS (
                    SELECT 1 FROM public.function_runs fr
                    WHERE fr.event_id = e.internal_id
                )
                AND NOT EXISTS (
                    SELECT 1 FROM public.history h
                    WHERE h.event_id = e.internal_id
                )
                AND NOT EXISTS (
                    SELECT 1 FROM public.event_batches eb 
                    WHERE POSITION(CAST(e.internal_id AS TEXT) IN CAST(eb.event_ids AS TEXT)) > 0
                )
                AND e.received_at < %s
                ORDER BY e.received_at ASC
                LIMIT %s
            )
            DELETE FROM public.events
            WHERE ctid IN (SELECT ctid FROM del)
            RETURNING 1;
        """, (cutoff, batch_size))
        deleted = cur.rowcount
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

def run_maintenance(conn):
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
            "idx_function_runs_event_id"
        ]
        
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

def main():
    logging.info("Starting batch cleanup process.")
    conn = get_db_connection()
    try:
        cutoff = datetime.now() - timedelta(days=30)
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
                    lograceging.info("Orphan events cleanup complete.")
                time.sleep(0.2)

            if tasks_state["trace_data"]:
                count = cleanup_trace_data(conn, cutoff)
                if count < BATCH_SIZE:
                    tasks_state["t_data"] = False
                    logging.info("Trace data cleanup complete.")
                time.sleep(0.2)

            logging.info("Current task state: %s", tasks_state)
            time.sleep(0.4)

        logging.info("All cleanup operations complete.")
        
        # Run maintenance in a separate connection with autocommit=True
        # run_maintenance(conn)

    except Exception as e:
        logging.exception("Error during cleanup: %s", e)
        conn.rollback()
    finally:
        conn.close()
        logging.info("Database connection closed.")

if __name__ == "__main__":
    main()