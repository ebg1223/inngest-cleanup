#!/usr/bin/env python3
"""
Database Cleanup Script - Direct SQL Implementation

This script directly executes SQL to clean up old events and orphaned records
without requiring stored procedures in the database.
"""

import os
import psycopg2
import sys
import time
from datetime import datetime, timedelta, UTC
import logging

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
    Parse configuration from environment variables
    Returns an object with the same attributes as the argparse version for compatibility
    """
    class EnvArgs:
        pass
    
    args = EnvArgs()
    
    # Database cleanup parameters
    args.hours = int(os.environ.get('CLEANUP_RETENTION_HOURS', '720'))
    args.batch_size = int(os.environ.get('CLEANUP_BATCH_SIZE', '1000'))
    args.runtime = int(os.environ.get('CLEANUP_MAX_RUNTIME_SECONDS', '300'))
    args.mode = os.environ.get('CLEANUP_MODE', 'all')
    
    # Validate mode
    if args.mode not in ['age', 'orphaned', 'all']:
        logger.warning(f"Invalid mode: {args.mode}. Using 'all' instead.")
        args.mode = 'all'
    
    # Database connection parameters
    args.db_host = os.environ.get('POSTGRES_HOST', 'localhost')
    args.db_port = int(os.environ.get('POSTGRES_PORT', '5432'))
    args.db_name = os.environ.get('POSTGRES_DB', 'dbname')
    args.db_user = os.environ.get('POSTGRES_USER', 'username')
    args.db_password = os.environ.get('POSTGRES_PASSWORD', 'password')
    
    logger.info(f"Configuration loaded from environment variables")
    return args

def cleanup_by_age(conn, retention_hours, batch_size, max_runtime_seconds):
    """Clean up old events and their dependent records based on age"""
    cursor = conn.cursor()
    start_time = time.time()
    end_time = start_time + max_runtime_seconds
    
    # Calculate cutoff date
    cutoff_date = datetime.now(UTC) - timedelta(hours=retention_hours)
    
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
                WHERE event_ts < %s
                ORDER BY event_ts ASC
                LIMIT %s
            """, (cutoff_date, batch_size))
            
            old_events = cursor.fetchall()
            
            if not old_events:
                logger.info("No more old events to delete")
                break
                
            # Extract internal_ids for the next query
            internal_ids = [event[0] for event in old_events]
            
            # Find function runs associated with these events
            cursor.execute("""
                SELECT run_id
                FROM function_runs
                WHERE event_id = ANY(%s)
            """, (internal_ids,))
            
            run_ids = [run[0] for run in cursor.fetchall()]
            
            # Start a transaction for this batch
            conn.autocommit = False
            
            # Delete function_finishes first
            if run_ids:
                cursor.execute("""
                    DELETE FROM function_finishes
                    WHERE run_id = ANY(%s)
                    RETURNING run_id
                """, (run_ids,))
                finishes_deleted = len(cursor.fetchall())
                total_finishes_deleted += finishes_deleted
                
                # Delete function_runs
                cursor.execute("""
                    DELETE FROM function_runs
                    WHERE run_id = ANY(%s)
                    RETURNING run_id
                """, (run_ids,))
                runs_deleted = len(cursor.fetchall())
                total_runs_deleted += runs_deleted
            else:
                finishes_deleted = 0
                runs_deleted = 0
            
            # Delete events
            cursor.execute("""
                DELETE FROM events
                WHERE internal_id = ANY(%s)
                RETURNING event_id
            """, (internal_ids,))
            events_deleted = len(cursor.fetchall())
            total_events_deleted += events_deleted
            
            # Commit the transaction
            conn.commit()
            conn.autocommit = True
            
            batch_duration = time.time() - batch_start
            logger.info(f"Batch {batch_count}: Deleted {events_deleted} events, {runs_deleted} runs, "
                        f"{finishes_deleted} finishes in {batch_duration:.2f} seconds")
            
            # Small pause to reduce resource contention
            time.sleep(0.1)
            
    except Exception as e:
        conn.rollback()
        logger.error(f"Error in age-based cleanup: {e}")
        raise
    finally:
        conn.autocommit = True
    
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
                LIMIT %s
            """, (batch_size,))
            
            orphaned_runs = cursor.fetchall()
            
            if not orphaned_runs:
                logger.info("No more orphaned function_runs found")
                break
                
            # Extract run_ids
            run_ids = [run[0] for run in orphaned_runs]
            
            # Start a transaction for this batch
            conn.autocommit = False
            
            # Delete dependent function_finishes first
            cursor.execute("""
                DELETE FROM function_finishes
                WHERE run_id = ANY(%s)
                RETURNING run_id
            """, (run_ids,))
            finishes_deleted = len(cursor.fetchall())
            related_finish_count += finishes_deleted
            
            # Delete orphaned function_runs
            cursor.execute("""
                DELETE FROM function_runs
                WHERE run_id = ANY(%s)
                RETURNING run_id
            """, (run_ids,))
            runs_deleted = len(cursor.fetchall())
            orphaned_run_count += runs_deleted
            
            # Commit the transaction
            conn.commit()
            conn.autocommit = True
            
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
                
                # Delete orphaned function_finishes
                cursor.execute("""
                    WITH orphaned_finishes AS (
                        SELECT f.run_id
                        FROM function_finishes f
                        LEFT JOIN function_runs r ON f.run_id = r.run_id
                        WHERE r.run_id IS NULL
                        LIMIT %s
                    )
                    DELETE FROM function_finishes
                    WHERE run_id IN (SELECT run_id FROM orphaned_finishes)
                    RETURNING run_id
                """, (batch_size,))
                
                finishes_deleted = len(cursor.fetchall())
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
    finally:
        conn.autocommit = True
    
    total_runtime = time.time() - start_time
    logger.info(f"Orphaned records cleanup completed in {total_runtime:.2f} seconds. "
                f"Deleted {orphaned_run_count} orphaned runs, {related_finish_count} related finishes, "
                f"{orphaned_finish_count} orphaned finishes")
    
    return {
        "orphaned_runs_deleted": orphaned_run_count,
        "related_finishes_deleted": related_finish_count,
        "orphaned_finishes_deleted": orphaned_finish_count
    }

def main():
    args = parse_args()
    
    logger.info(f"Database cleanup started at {datetime.now(UTC).isoformat()}")
    logger.info(f"Using configuration: retention={args.hours}h, batch_size={args.batch_size}, "
                f"runtime={args.runtime}s, mode={args.mode}")
    results = {}
    
    try:
        # Connect to the database
        logger.info(f"Connecting to database: {args.db_host}:{args.db_port}/{args.db_name}")
        conn = psycopg2.connect(
            host=args.db_host,
            port=args.db_port,
            dbname=args.db_name,
            user=args.db_user,
            password=args.db_password
        )
        
        # Ensure autocommit is on by default
        conn.autocommit = True
        
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
    # conn = psycopg2.connect("postgresql://postgres:postgrespassword@10.138.0.52/myapp")
    # cursor = conn.cursor()

    # Find event IDs to delete
    # cursor.execute("""
    #     SELECT internal_id
    #     FROM events
    #     ORDER BY event_ts ASC
    #     LIMIT 10
    # """)
    
    # old_events = cursor.fetchall()
    # print(old_events)
    sys.exit(main())