#!/usr/bin/env python3
"""
Database Cleanup Script - Direct SQL Implementation for SQLite

This script directly executes SQL to clean up old events and orphaned records
in an SQLite database without requiring stored procedures.
"""

import os
import sqlite3
import sys
import time
from datetime import datetime, timedelta, UTC
import logging
import pathlib

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        # logging.FileHandler("db_cleanup.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def parse_args():
    """
    Parse configuration from environment variables.
    Returns an object with the same attributes as the argparse version for compatibility.
    """
    class EnvArgs:
        pass
    
    args = EnvArgs()
    
    # Database cleanup parameters
    args.hours = int(os.environ.get('CLEANUP_RETENTION_HOURS', '720'))
    args.batch_size = int(os.environ.get('CLEANUP_BATCH_SIZE', '1000'))
    args.runtime = int(os.environ.get('CLEANUP_MAX_RUNTIME_SECONDS', '300'))
    args.mode = os.environ.get('CLEANUP_MODE', 'all')
    args.create_indexes = os.environ.get('CREATE_INDEXES', 'true')
    
    # Validate mode
    if args.mode not in ['age', 'orphaned', 'all']:
        logger.warning(f"Invalid mode: {args.mode}. Using 'all' instead.")
        args.mode = 'all'
    
    # Database connection parameters - for SQLite we only need the database path
    args.db_path = os.environ.get('SQLITE_DB_PATH', 'events.db')
    
    logger.info("Configuration loaded from environment variables")
    return args

def cleanup_by_age(conn, retention_hours, batch_size, max_runtime_seconds):
    """Clean up old events and their dependent records based on age"""
    cursor = conn.cursor()
    start_time = time.time()
    end_time = start_time + max_runtime_seconds
    
    # Calculate cutoff date
    cutoff_date = datetime.now(UTC) - timedelta(hours=retention_hours)
    cutoff_iso = cutoff_date.isoformat()
    
    logger.info(f"Starting age-based cleanup. Retention: {retention_hours} hours. Cutoff date: {cutoff_date}")
    
    total_events_deleted = 0
    total_runs_deleted = 0
    total_finishes_deleted = 0
    batch_count = 0
    
    try:
        while time.time() < end_time:
            batch_count += 1
            batch_start = time.time()
            
            # Find event IDs to delete
            cursor.execute("""
                SELECT internal_id
                FROM events
                WHERE event_ts < ?
                ORDER BY event_ts ASC
                LIMIT ?
            """, (cutoff_iso, batch_size))
            
            old_events = cursor.fetchall()
            
            if not old_events:
                logger.info("No more old events to delete")
                break
                
            # Extract internal_ids for the next query
            internal_ids = [event[0] for event in old_events]
            internal_ids_str = ', '.join(['?' for _ in internal_ids])
            
            # Find function runs associated with these events
            cursor.execute(f"""
                SELECT run_id
                FROM function_runs
                WHERE event_id IN ({internal_ids_str})
            """, internal_ids)
            
            run_ids = [run[0] for run in cursor.fetchall()]
            
            # Start a transaction for this batch
            conn.execute("BEGIN TRANSACTION")
            
            # Delete function_finishes first
            if run_ids:
                run_ids_str = ', '.join(['?' for _ in run_ids])
                
                # Delete function_finishes - SQLite doesn't support RETURNING clause
                cursor.execute(f"""
                    DELETE FROM function_finishes
                    WHERE run_id IN ({run_ids_str})
                """, run_ids)
                finishes_deleted = cursor.rowcount
                total_finishes_deleted += finishes_deleted
                
                # Delete function_runs
                cursor.execute(f"""
                    DELETE FROM function_runs
                    WHERE run_id IN ({run_ids_str})
                """, run_ids)
                runs_deleted = cursor.rowcount
                total_runs_deleted += runs_deleted
            else:
                finishes_deleted = 0
                runs_deleted = 0
            
            # Delete events
            cursor.execute(f"""
                DELETE FROM events
                WHERE internal_id IN ({internal_ids_str})
            """, internal_ids)
            events_deleted = cursor.rowcount
            total_events_deleted += events_deleted
            
            # Commit the transaction
            conn.commit()
            
            batch_duration = time.time() - batch_start
            logger.info(f"Batch {batch_count}: Deleted {events_deleted} events, {runs_deleted} runs, "
                        f"{finishes_deleted} finishes in {batch_duration:.2f} seconds")
            
            # Small pause to reduce resource contention
            time.sleep(0.4)
            
    except Exception as e:
        conn.rollback()
        logger.error(f"Error in age-based cleanup: {e}")
        raise
    
    total_runtime = time.time() - start_time
    logger.info(f"Age-based cleanup completed in {total_runtime:.2f} seconds. "
                f"Deleted {total_events_deleted} events, {total_runs_deleted} runs, "
                f"{total_finishes_deleted} finishes")
    
    return {
        "events_deleted": total_events_deleted,
        "runs_deleted": total_runs_deleted,
        "finishes_deleted": total_finishes_deleted
    }

def cleanup_orphaned_records(conn, batch_size, max_runtime_seconds):
    """Clean up orphaned function_runs and function_finishes"""
    cursor = conn.cursor()
    start_time = time.time()
    end_time = start_time + max_runtime_seconds
    
    logger.info("Starting orphaned records cleanup")
    
    orphaned_run_count = 0
    related_finish_count = 0
    orphaned_finish_count = 0
    batch_count = 0
    
    try:
        # Phase 1: Clean up orphaned function_runs and their dependent function_finishes
        logger.info("Phase 1: Cleaning up orphaned function_runs")
        batch_count = 0
        
        while time.time() < end_time:
            batch_count += 1
            batch_start = time.time()
            
            # Find orphaned function_runs
            cursor.execute("""
                SELECT r.run_id
                FROM function_runs r
                LEFT JOIN events e ON r.event_id = e.internal_id
                WHERE e.internal_id IS NULL
                LIMIT ?
            """, (batch_size,))
            
            orphaned_runs = cursor.fetchall()
            
            if not orphaned_runs:
                logger.info("No more orphaned function_runs found")
                break
                
            # Extract run_ids
            run_ids = [run[0] for run in orphaned_runs]
            run_ids_str = ', '.join(['?' for _ in run_ids])
            
            # Start a transaction for this batch
            conn.execute("BEGIN TRANSACTION")
            
            # Delete dependent function_finishes first
            cursor.execute(f"""
                DELETE FROM function_finishes
                WHERE run_id IN ({run_ids_str})
            """, run_ids)
            finishes_deleted = cursor.rowcount
            related_finish_count += finishes_deleted
            
            # Delete orphaned function_runs
            cursor.execute(f"""
                DELETE FROM function_runs
                WHERE run_id IN ({run_ids_str})
            """, run_ids)
            runs_deleted = cursor.rowcount
            orphaned_run_count += runs_deleted
            
            # Commit the transaction
            conn.commit()
            
            batch_duration = time.time() - batch_start
            logger.info(f"Batch {batch_count}: Deleted {runs_deleted} orphaned runs and "
                        f"{finishes_deleted} related finishes in {batch_duration:.2f} seconds")
            
            # Small pause to reduce resource contention
            time.sleep(0.1)
        
        # Phase 2: Clean up orphaned function_finishes
        if time.time() < end_time:
            logger.info("Phase 2: Cleaning up orphaned function_finishes")
            batch_count = 0
            
            while time.time() < end_time:
                batch_count += 1
                batch_start = time.time()
                
                # Delete orphaned function_finishes using a subquery
                cursor.execute("""
                    DELETE FROM function_finishes
                    WHERE run_id IN (
                        SELECT f.run_id
                        FROM function_finishes f
                        LEFT JOIN function_runs r ON f.run_id = r.run_id
                        WHERE r.run_id IS NULL
                        LIMIT ?
                    )
                """, (batch_size,))
                
                finishes_deleted = cursor.rowcount
                orphaned_finish_count += finishes_deleted
                
                if finishes_deleted == 0:
                    logger.info("No more orphaned function_finishes found")
                    break
                
                batch_duration = time.time() - batch_start
                logger.info(f"Batch {batch_count}: Deleted {finishes_deleted} orphaned finishes "
                            f"in {batch_duration:.2f} seconds")
                
                # Small pause to reduce resource contention
                time.sleep(0.1)
        
    except Exception as e:
        conn.rollback()
        logger.error(f"Error in orphaned records cleanup: {e}")
        raise
    
    total_runtime = time.time() - start_time
    logger.info(f"Orphaned records cleanup completed in {total_runtime:.2f} seconds. "
                f"Deleted {orphaned_run_count} orphaned runs, {related_finish_count} related finishes, "
                f"{orphaned_finish_count} orphaned finishes")
    
    return {
        "orphaned_runs_deleted": orphaned_run_count,
        "related_finishes_deleted": related_finish_count,
        "orphaned_finishes_deleted": orphaned_finish_count
    }

def check_and_create_indexes(conn):
    """Check if necessary indexes exist and create them if needed"""
    cursor = conn.cursor()
    created_indexes = []
    
    # Define the indexes we need
    required_indexes = [
        {
            "name": "idx_events_event_ts",
            "table": "events",
            "columns": "event_ts",
            "type": ""
        },
        {
            "name": "idx_events_internal_id",
            "table": "events",
            "columns": "internal_id",
            "type": ""
        },
        {
            "name": "idx_function_runs_event_id",
            "table": "function_runs",
            "columns": "event_id",
            "type": ""
        },
        {
            "name": "idx_function_finishes_run_id",
            "table": "function_finishes",
            "columns": "run_id",
            "type": ""
        }
    ]
    
    try:
        # Check which indexes exist - SQLite approach
        cursor.execute("""
            SELECT name FROM sqlite_master 
            WHERE type = 'index' AND sql IS NOT NULL
        """)
        
        existing_indexes = [row[0] for row in cursor.fetchall()]
        logger.info(f"Found {len(existing_indexes)} existing indexes")
        
        # Create any missing indexes
        for idx in required_indexes:
            if idx["name"] not in existing_indexes:
                logger.info(f"Creating missing index: {idx['name']} on {idx['table']}({idx['columns']})")
                
                # Build the CREATE INDEX statement
                create_statement = f"""
                    CREATE INDEX {idx['name']} 
                    ON {idx['table']}({idx['columns']})
                    {idx['type']}
                """
                
                # Execute CREATE INDEX
                cursor.execute(create_statement)
                created_indexes.append(idx["name"])
                logger.info(f"Created index: {idx['name']}")
            else:
                logger.info(f"Index {idx['name']} already exists")
        
        conn.commit()
        return created_indexes
        
    except Exception as e:
        conn.rollback()
        logger.error(f"Error creating indexes: {e}")
        raise

def main():
    args = parse_args()
    
    logger.info(f"Database cleanup started at {datetime.now(UTC).isoformat()}")
    logger.info(f"Using configuration: retention={args.hours}h, batch_size={args.batch_size}, "
                f"runtime={args.runtime}s, mode={args.mode}")
    results = {}
    
    try:
        # Make sure the database directory exists
        db_dir = pathlib.Path(args.db_path).parent
        db_dir.mkdir(parents=True, exist_ok=True)
        
        # Connect to the SQLite database with a busy timeout set (5000ms)
        logger.info(f"Connecting to SQLite database: {args.db_path}")
        conn = sqlite3.connect(args.db_path, timeout=5.0)
        conn.execute("PRAGMA busy_timeout = 5000")
        
        # Enable foreign keys
        conn.execute("PRAGMA foreign_keys = ON")

        if args.create_indexes == 'true':
            check_and_create_indexes(conn)
        
        # Run the selected cleanup mode(s)
        if args.mode in ['age', 'all']:
            results['age_cleanup'] = cleanup_by_age(
                conn, args.hours, args.batch_size, 
                args.runtime if args.mode == 'age' else args.runtime // 2
            )
        
        if args.mode in ['orphaned', 'all']:
            results['orphaned_cleanup'] = cleanup_orphaned_records(
                conn, args.batch_size,
                args.runtime if args.mode == 'orphaned' else args.runtime // 2
            )
        
        conn.close()
        
        # Summarize results
        logger.info("Cleanup Summary:")
        for mode, result in results.items():
            logger.info(f"  {mode}: {result}")
        
        logger.info(f"Database cleanup completed successfully at {datetime.now(UTC).isoformat()}")
        return 0
        
    except Exception as e:
        logger.error(f"Database cleanup failed: {e}")
        logger.info(f"Database cleanup failed at {datetime.now(UTC).isoformat()}")
        return 1

if __name__ == "__main__":
    sys.exit(main())
