#!/usr/bin/env python3
"""
Inngest Database Cleanup Script - Environment Variable Version

This version uses environment variables for configuration to work well in Docker.
"""

import logging
import os
import sys
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Set
import psycopg2
import sqlite3
from urllib.parse import urlparse

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ImprovedInngestCleaner:
    """Improved cleanup with better handling of edge cases."""
    
    def __init__(self, database_url: str, retention_days: int, dry_run: bool = False,
                 batch_size: int = 1000, double_retention_multiplier: float = 2.0):
        self.database_url = database_url
        self.retention_days = retention_days
        self.dry_run = dry_run
        self.batch_size = batch_size
        self.double_retention_multiplier = double_retention_multiplier
        self.db_type = self._determine_db_type(database_url)
        self.connection = None
        
        # Calculate cutoff dates
        self.cutoff_date = datetime.now() - timedelta(days=retention_days)
        self.double_cutoff_date = datetime.now() - timedelta(days=retention_days * double_retention_multiplier)
        
        logger.info(f"Database type: {self.db_type}")
        logger.info(f"Standard retention: {retention_days} days (cutoff: {self.cutoff_date})")
        logger.info(f"Extended retention: {retention_days * double_retention_multiplier} days (cutoff: {self.double_cutoff_date})")
        logger.info(f"Batch size: {batch_size}")
        logger.info(f"Dry run: {dry_run}")
    
    def _determine_db_type(self, url: str) -> str:
        """Determine if the database is PostgreSQL or SQLite."""
        parsed = urlparse(url)
        if parsed.scheme in ('postgresql', 'postgres'):
            return 'postgresql'
        elif parsed.scheme in ('sqlite', '') and (url.startswith('sqlite:') or url.endswith('.db')):
            return 'sqlite'
        else:
            raise ValueError(f"Unsupported database type: {parsed.scheme}")
    
    def connect(self):
        """Establish database connection."""
        if self.db_type == 'postgresql':
            self.connection = psycopg2.connect(self.database_url)
        else:
            # Handle SQLite URL format
            if self.database_url.startswith('sqlite:///'):
                db_path = self.database_url[10:]
            elif self.database_url.startswith('sqlite:'):
                db_path = self.database_url[7:]
            else:
                db_path = self.database_url
            self.connection = sqlite3.connect(db_path)
            # Enable foreign key constraints
            self.connection.execute("PRAGMA foreign_keys = ON")
        logger.info(f"Connected to {self.db_type} database")
    
    def disconnect(self):
        """Close database connection."""
        if self.connection:
            self.connection.close()
            logger.info("Database connection closed")
    
    def get_function_runs_to_delete(self) -> List[str]:
        """
        Get function run IDs to delete using improved logic:
        1. Function finishes older than cutoff with no active child runs
        2. Function runs older than 2x cutoff (for stuck/incomplete runs)
        """
        cursor = self.connection.cursor()
        run_ids = []
        
        try:
            if self.db_type == 'postgresql':
                query = """
                    SELECT DISTINCT run_id FROM (
                        -- Completed runs older than retention
                        SELECT ff.run_id
                        FROM function_finishes ff
                        WHERE ff.created_at < %s
                          -- Check for child runs that are still within retention
                          AND NOT EXISTS (
                              SELECT 1
                              FROM function_runs fr
                              LEFT JOIN function_finishes ff2 ON fr.run_id = ff2.run_id
                              WHERE fr.original_run_id = ff.run_id
                                AND (ff2.run_id IS NULL OR ff2.created_at >= %s)
                          )
                        
                        UNION
                        
                        -- Stuck/incomplete runs older than 2x retention
                        SELECT fr.run_id
                        FROM function_runs fr
                        WHERE fr.run_started_at < %s
                    ) AS runs_to_delete
                    ORDER BY run_id
                    LIMIT %s
                """
                params = (self.cutoff_date, self.cutoff_date, self.double_cutoff_date, self.batch_size)
            else:
                # SQLite version
                query = """
                    SELECT DISTINCT run_id FROM (
                        -- Completed runs older than retention
                        SELECT ff.run_id
                        FROM function_finishes ff
                        WHERE ff.created_at < ?
                          AND NOT EXISTS (
                              SELECT 1
                              FROM function_runs fr
                              LEFT JOIN function_finishes ff2 ON fr.run_id = ff2.run_id
                              WHERE fr.original_run_id = ff.run_id
                                AND (ff2.run_id IS NULL OR ff2.created_at >= ?)
                          )
                        
                        UNION
                        
                        -- Stuck/incomplete runs older than 2x retention
                        SELECT fr.run_id
                        FROM function_runs fr
                        WHERE fr.run_started_at < ?
                    ) AS runs_to_delete
                    ORDER BY run_id
                    LIMIT ?
                """
                params = (self.cutoff_date, self.cutoff_date, self.double_cutoff_date, self.batch_size)
            
            cursor.execute(query, params)
            run_ids = [row[0] for row in cursor.fetchall()]
            
        except Exception as e:
            logger.error(f"Error finding function runs to delete: {e}")
        finally:
            cursor.close()
        
        return run_ids
    
    def get_orphaned_history_ids(self) -> List[str]:
        """Find history records that have no corresponding function_run."""
        cursor = self.connection.cursor()
        history_ids = []
        
        try:
            if self.db_type == 'postgresql':
                query = """
                    SELECT h.id
                    FROM history h
                    WHERE h.created_at < %s
                      AND NOT EXISTS (
                          SELECT 1 FROM function_runs fr
                          WHERE fr.run_id = h.run_id
                      )
                    LIMIT %s
                """
                params = (self.cutoff_date, self.batch_size)
            else:
                query = """
                    SELECT h.id
                    FROM history h
                    WHERE h.created_at < ?
                      AND NOT EXISTS (
                          SELECT 1 FROM function_runs fr
                          WHERE fr.run_id = h.run_id
                      )
                    LIMIT ?
                """
                params = (self.cutoff_date, self.batch_size)
            
            cursor.execute(query, params)
            history_ids = [row[0] for row in cursor.fetchall()]
            
        except Exception as e:
            logger.error(f"Error finding orphaned history: {e}")
        finally:
            cursor.close()
        
        return history_ids
    
    def get_orphaned_function_finishes(self) -> List[str]:
        """Find function_finishes records that have no corresponding function_run."""
        cursor = self.connection.cursor()
        run_ids = []
        
        try:
            if self.db_type == 'postgresql':
                query = """
                    SELECT ff.run_id
                    FROM function_finishes ff
                    WHERE ff.created_at < %s
                      AND NOT EXISTS (
                          SELECT 1 FROM function_runs fr
                          WHERE fr.run_id = ff.run_id
                      )
                    LIMIT %s
                """
                params = (self.cutoff_date, self.batch_size)
            else:
                query = """
                    SELECT ff.run_id
                    FROM function_finishes ff
                    WHERE ff.created_at < ?
                      AND NOT EXISTS (
                          SELECT 1 FROM function_runs fr
                          WHERE fr.run_id = ff.run_id
                      )
                    LIMIT ?
                """
                params = (self.cutoff_date, self.batch_size)
            
            cursor.execute(query, params)
            run_ids = [row[0] for row in cursor.fetchall()]
            
        except Exception as e:
            logger.error(f"Error finding orphaned function finishes: {e}")
        finally:
            cursor.close()
        
        return run_ids
    
    def get_referenced_event_ids(self, cutoff: datetime) -> Set[str]:
        """Get all event IDs that are still referenced by function runs or history."""
        cursor = self.connection.cursor()
        event_ids = set()
        
        try:
            if self.db_type == 'postgresql':
                query = """
                    SELECT DISTINCT event_id FROM (
                        SELECT event_id FROM function_runs 
                        WHERE event_id IS NOT NULL
                          AND run_started_at >= %s
                        UNION
                        SELECT event_id FROM history 
                        WHERE event_id IS NOT NULL
                          AND created_at >= %s
                    ) AS referenced_events
                """
                params = (cutoff, cutoff)
            else:
                query = """
                    SELECT DISTINCT event_id FROM (
                        SELECT event_id FROM function_runs 
                        WHERE event_id IS NOT NULL
                          AND run_started_at >= ?
                        UNION
                        SELECT event_id FROM history 
                        WHERE event_id IS NOT NULL
                          AND created_at >= ?
                    ) AS referenced_events
                """
                params = (cutoff, cutoff)
            
            cursor.execute(query, params)
            event_ids = {row[0] for row in cursor.fetchall() if row[0]}
            
        except Exception as e:
            logger.error(f"Error finding referenced events: {e}")
        finally:
            cursor.close()
        
        return event_ids
    
    def clean_function_data(self) -> Dict[str, int]:
        """Clean function-related data with improved logic."""
        stats = {
            'function_runs': 0,
            'function_finishes': 0,
            'history': 0,
            'orphaned_history': 0,
            'orphaned_finishes': 0
        }
        
        if self.dry_run:
            logger.info("[DRY RUN] Would clean function data")
            return stats
        
        # Clean based on run IDs
        run_ids = self.get_function_runs_to_delete()
        if run_ids:
            cursor = self.connection.cursor()
            try:
                if self.db_type == 'postgresql':
                    # Delete history first (child records)
                    cursor.execute(
                        "DELETE FROM history WHERE run_id = ANY(%s)",
                        (run_ids,)
                    )
                    stats['history'] = cursor.rowcount
                    
                    # Delete function finishes
                    cursor.execute(
                        "DELETE FROM function_finishes WHERE run_id = ANY(%s)",
                        (run_ids,)
                    )
                    stats['function_finishes'] = cursor.rowcount
                    
                    # Delete function runs last (parent records)
                    cursor.execute(
                        "DELETE FROM function_runs WHERE run_id = ANY(%s)",
                        (run_ids,)
                    )
                    stats['function_runs'] = cursor.rowcount
                else:
                    # SQLite version
                    placeholders = ','.join(['?' for _ in run_ids])
                    
                    cursor.execute(
                        f"DELETE FROM history WHERE run_id IN ({placeholders})",
                        run_ids
                    )
                    stats['history'] = cursor.rowcount
                    
                    cursor.execute(
                        f"DELETE FROM function_finishes WHERE run_id IN ({placeholders})",
                        run_ids
                    )
                    stats['function_finishes'] = cursor.rowcount
                    
                    cursor.execute(
                        f"DELETE FROM function_runs WHERE run_id IN ({placeholders})",
                        run_ids
                    )
                    stats['function_runs'] = cursor.rowcount
                
                self.connection.commit()
                cursor.close()
                
            except Exception as e:
                self.connection.rollback()
                logger.error(f"Error cleaning function data: {e}")
                cursor.close()
                return stats
        
        # Clean orphaned history records
        history_ids = self.get_orphaned_history_ids()
        if history_ids:
            cursor = self.connection.cursor()
            try:
                if self.db_type == 'postgresql':
                    cursor.execute(
                        "DELETE FROM history WHERE id = ANY(%s)",
                        (history_ids,)
                    )
                else:
                    placeholders = ','.join(['?' for _ in history_ids])
                    cursor.execute(
                        f"DELETE FROM history WHERE id IN ({placeholders})",
                        history_ids
                    )
                
                stats['orphaned_history'] = cursor.rowcount
                self.connection.commit()
                cursor.close()
                
            except Exception as e:
                self.connection.rollback()
                logger.error(f"Error cleaning orphaned history: {e}")
                cursor.close()
        
        # Clean orphaned function_finishes
        finish_run_ids = self.get_orphaned_function_finishes()
        if finish_run_ids:
            cursor = self.connection.cursor()
            try:
                if self.db_type == 'postgresql':
                    cursor.execute(
                        "DELETE FROM function_finishes WHERE run_id = ANY(%s)",
                        (finish_run_ids,)
                    )
                else:
                    placeholders = ','.join(['?' for _ in finish_run_ids])
                    cursor.execute(
                        f"DELETE FROM function_finishes WHERE run_id IN ({placeholders})",
                        finish_run_ids
                    )
                
                stats['orphaned_finishes'] = cursor.rowcount
                self.connection.commit()
                cursor.close()
                
            except Exception as e:
                self.connection.rollback()
                logger.error(f"Error cleaning orphaned finishes: {e}")
                cursor.close()
        
        return stats
    
    def clean_events(self) -> int:
        """Clean old events that are not referenced."""
        if self.dry_run:
            logger.info("[DRY RUN] Would clean events")
            return 0
        
        # Get referenced event IDs
        referenced_events = self.get_referenced_event_ids(self.cutoff_date)
        
        cursor = self.connection.cursor()
        deleted = 0
        
        try:
            if self.db_type == 'postgresql':
                # Use a temporary table for better performance
                cursor.execute("CREATE TEMP TABLE temp_referenced_events (event_id TEXT)")
                
                # Insert referenced events in batches
                if referenced_events:
                    # Convert set to list for batching
                    ref_list = list(referenced_events)
                    for i in range(0, len(ref_list), 1000):
                        batch = ref_list[i:i+1000]
                        cursor.executemany(
                            "INSERT INTO temp_referenced_events VALUES (%s)",
                            [(e,) for e in batch]
                        )
                
                # Delete old, unreferenced events
                cursor.execute("""
                    DELETE FROM events e
                    WHERE e.received_at < %s
                      AND NOT EXISTS (
                          SELECT 1 FROM temp_referenced_events t
                          WHERE t.event_id = e.internal_id
                      )
                      AND e.internal_id IN (
                          SELECT internal_id FROM events
                          WHERE received_at < %s
                          LIMIT %s
                      )
                """, (self.cutoff_date, self.cutoff_date, self.batch_size))
                
                deleted = cursor.rowcount
                cursor.execute("DROP TABLE temp_referenced_events")
                
            else:
                # SQLite version - use rowid approach
                cursor.execute("CREATE TEMP TABLE temp_referenced_events (event_id TEXT)")
                
                if referenced_events:
                    # Insert referenced events
                    cursor.executemany(
                        "INSERT INTO temp_referenced_events VALUES (?)",
                        [(e,) for e in referenced_events]
                    )
                
                # Find events to delete
                cursor.execute("""
                    CREATE TEMP TABLE temp_delete_events AS
                    SELECT e.rowid AS row_id
                    FROM events e
                    WHERE e.received_at < ?
                      AND NOT EXISTS (
                          SELECT 1 FROM temp_referenced_events t
                          WHERE t.event_id = e.internal_id
                      )
                    LIMIT ?
                """, (self.cutoff_date, self.batch_size))
                
                # Delete using rowids
                cursor.execute("""
                    DELETE FROM events
                    WHERE rowid IN (SELECT row_id FROM temp_delete_events)
                """)
                
                deleted = cursor.rowcount
                cursor.execute("DROP TABLE temp_referenced_events")
                cursor.execute("DROP TABLE temp_delete_events")
            
            self.connection.commit()
            
        except Exception as e:
            self.connection.rollback()
            logger.error(f"Error cleaning events: {e}")
        finally:
            cursor.close()
        
        return deleted
    
    def clean_traces(self) -> Dict[str, int]:
        """Clean old traces and trace runs."""
        stats = {'traces': 0, 'trace_runs': 0}
        
        if self.dry_run:
            logger.info("[DRY RUN] Would clean trace data")
            return stats
        
        cursor = self.connection.cursor()
        
        try:
            # Find trace runs to delete based on ended_at
            if self.db_type == 'postgresql':
                cursor.execute("""
                    SELECT run_id FROM trace_runs
                    WHERE ended_at IS NOT NULL
                      AND ended_at < %s
                    LIMIT %s
                """, (self.cutoff_date, self.batch_size))
            else:
                # For SQLite, handle milliseconds if that's how it's stored
                cutoff_ms = int(self.cutoff_date.timestamp() * 1000)
                cursor.execute("""
                    SELECT run_id FROM trace_runs
                    WHERE ended_at IS NOT NULL
                      AND ended_at < ?
                    LIMIT ?
                """, (cutoff_ms, self.batch_size))
            
            run_ids = [row[0] for row in cursor.fetchall()]
            
            if run_ids:
                if self.db_type == 'postgresql':
                    # Delete traces first
                    cursor.execute(
                        "DELETE FROM traces WHERE run_id = ANY(%s)",
                        (run_ids,)
                    )
                    stats['traces'] = cursor.rowcount
                    
                    # Delete trace runs
                    cursor.execute(
                        "DELETE FROM trace_runs WHERE run_id = ANY(%s)",
                        (run_ids,)
                    )
                    stats['trace_runs'] = cursor.rowcount
                else:
                    placeholders = ','.join(['?' for _ in run_ids])
                    
                    cursor.execute(
                        f"DELETE FROM traces WHERE run_id IN ({placeholders})",
                        run_ids
                    )
                    stats['traces'] = cursor.rowcount
                    
                    cursor.execute(
                        f"DELETE FROM trace_runs WHERE run_id IN ({placeholders})",
                        run_ids
                    )
                    stats['trace_runs'] = cursor.rowcount
            
            self.connection.commit()
            
        except Exception as e:
            self.connection.rollback()
            logger.error(f"Error cleaning traces: {e}")
        finally:
            cursor.close()
        
        return stats
    
    def run_cleanup(self):
        """Execute the full cleanup process."""
        try:
            self.connect()
            
            logger.info("Starting improved cleanup process...")
            
            # Track what needs more cleanup
            needs_more = {
                'function_data': True,
                'events': True,
                'traces': True
            }
            
            total_stats = {
                'function_runs': 0,
                'function_finishes': 0,
                'history': 0,
                'orphaned_history': 0,
                'orphaned_finishes': 0,
                'events': 0,
                'traces': 0,
                'trace_runs': 0
            }
            
            # Keep cleaning until nothing left to clean
            while any(needs_more.values()):
                
                # Clean function data
                if needs_more['function_data']:
                    logger.info("Cleaning function data...")
                    stats = self.clean_function_data()
                    
                    # Update totals
                    for key, value in stats.items():
                        total_stats[key] += value
                    
                    # Check if we need to continue
                    total_cleaned = sum(stats.values())
                    if total_cleaned < self.batch_size:
                        needs_more['function_data'] = False
                        logger.info("Function data cleanup complete for this run")
                    else:
                        logger.info(f"Cleaned {total_cleaned} function-related records")
                
                # Clean events
                if needs_more['events']:
                    logger.info("Cleaning events...")
                    deleted = self.clean_events()
                    total_stats['events'] += deleted
                    
                    if deleted < self.batch_size:
                        needs_more['events'] = False
                        logger.info("Event cleanup complete for this run")
                    else:
                        logger.info(f"Cleaned {deleted} event records")
                
                # Clean traces
                if needs_more['traces']:
                    logger.info("Cleaning trace data...")
                    stats = self.clean_traces()
                    
                    total_stats['traces'] += stats['traces']
                    total_stats['trace_runs'] += stats['trace_runs']
                    
                    total_cleaned = sum(stats.values())
                    if total_cleaned < self.batch_size:
                        needs_more['traces'] = False
                        logger.info("Trace cleanup complete for this run")
                    else:
                        logger.info(f"Cleaned {total_cleaned} trace-related records")
                
                # Small delay between batches
                if any(needs_more.values()):
                    import time
                    time.sleep(0.5)
            
            # Print summary
            logger.info("\nCleanup Summary:")
            logger.info("-" * 40)
            for table, count in total_stats.items():
                if count > 0:
                    logger.info(f"{table}: {count:,} records deleted")
            logger.info(f"Total records deleted: {sum(total_stats.values()):,}")
            
            # Vacuum if not dry run and records were deleted
            if not self.dry_run and sum(total_stats.values()) > 0:
                self.vacuum_database()
            
        finally:
            self.disconnect()
    
    def vacuum_database(self):
        """Run VACUUM to reclaim disk space."""
        logger.info("Running VACUUM to reclaim disk space...")
        cursor = self.connection.cursor()
        
        try:
            if self.db_type == 'postgresql':
                # Need to close the transaction before VACUUM
                self.connection.commit()
                old_isolation_level = self.connection.isolation_level
                self.connection.set_isolation_level(0)
                cursor.execute("VACUUM ANALYZE")
                self.connection.set_isolation_level(old_isolation_level)
            else:
                cursor.execute("VACUUM")
            
            logger.info("VACUUM completed successfully")
            
        except Exception as e:
            logger.error(f"Error during vacuum: {e}")
        finally:
            cursor.close()


def main():
    """Main function that reads configuration from environment variables."""
    
    # Read configuration from environment variables
    database_url = os.getenv('INNGEST_DATABASE_URL')
    retention_days = int(os.getenv('INNGEST_RETENTION_DAYS', '30'))
    dry_run = os.getenv('INNGEST_DRY_RUN', 'false').lower() in ('true', '1', 'yes')
    batch_size = int(os.getenv('INNGEST_BATCH_SIZE', '1000'))
    double_retention = float(os.getenv('INNGEST_DOUBLE_RETENTION', '2.0'))
    
    # Validate required configuration
    if not database_url:
        logger.error("INNGEST_DATABASE_URL environment variable is required")
        sys.exit(1)
    
    if retention_days < 1:
        logger.error("INNGEST_RETENTION_DAYS must be at least 1")
        sys.exit(1)
    
    if double_retention < 1:
        logger.error("INNGEST_DOUBLE_RETENTION must be at least 1")
        sys.exit(1)
    
    # Log configuration
    logger.info("Inngest Cleanup Configuration:")
    logger.info(f"  Database URL: {database_url[:20]}...")  # Log partial URL for security
    logger.info(f"  Retention days: {retention_days}")
    logger.info(f"  Dry run: {dry_run}")
    logger.info(f"  Batch size: {batch_size}")
    logger.info(f"  Double retention multiplier: {double_retention}")
    
    try:
        cleaner = ImprovedInngestCleaner(
            database_url=database_url,
            retention_days=retention_days,
            dry_run=dry_run,
            batch_size=batch_size,
            double_retention_multiplier=double_retention
        )
        cleaner.run_cleanup()
        logger.info("Cleanup completed successfully")
    except Exception as e:
        logger.error(f"Cleanup failed: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()