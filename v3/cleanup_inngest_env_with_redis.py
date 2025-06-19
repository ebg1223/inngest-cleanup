#!/usr/bin/env python3
"""
Inngest Database Cleanup Script with Redis Awareness - PostgreSQL Type Fix

This version handles PostgreSQL bytea types and bigint timestamps correctly.
"""

import logging
import os
import sys
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Set, Optional
import psycopg2
import sqlite3
import redis
from urllib.parse import urlparse
import json

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class RedisAwareInngestCleaner:
    """Redis-aware cleanup that intelligently handles completed vs incomplete runs.
    
    Logic:
    - Completed runs (with function_finishes): Safe to delete if older than retention
      (Redis state is already cleaned by Inngest on completion)
    - Incomplete runs (no function_finishes): Only delete if no Redis state AND older than 2x retention
      (Presence of Redis state means still executing)
    """
    
    def __init__(self, database_url: str, redis_url: str, retention_days: int, 
                 dry_run: bool = False, batch_size: int = 1000, 
                 double_retention_multiplier: float = 2.0,
                 redis_key_prefix: str = "inngest"):
        self.database_url = database_url
        self.redis_url = redis_url
        self.retention_days = retention_days
        self.dry_run = dry_run
        self.batch_size = batch_size
        self.double_retention_multiplier = double_retention_multiplier
        self.redis_key_prefix = redis_key_prefix
        self.db_type = self._determine_db_type(database_url)
        self.connection = None
        self.redis_client = None
        
        # Calculate cutoff dates
        self.cutoff_date = datetime.now() - timedelta(days=retention_days)
        self.double_cutoff_date = datetime.now() - timedelta(days=retention_days * double_retention_multiplier)
        
        # Redis key patterns based on ACTUAL Inngest deployment
        self.redis_patterns = {
            'pauses': f"{{{redis_key_prefix}}}:pauses:*",
            'pause_runs': f"{{{redis_key_prefix}}}:pr:*",
            'metadata': f"{{{redis_key_prefix}:*}}:metadata:*",
            'stack': f"{{{redis_key_prefix}:*}}:stack:*",
            'actions': f"{{{redis_key_prefix}:*}}:actions:*",
            'events': f"{{{redis_key_prefix}:*}}:events:*",
        }
        
        logger.info(f"Database type: {self.db_type}")
        logger.info(f"Redis URL: {redis_url[:20]}...")
        logger.info(f"Standard retention: {retention_days} days (cutoff: {self.cutoff_date})")
        logger.info(f"Extended retention: {retention_days * double_retention_multiplier} days (cutoff: {self.double_cutoff_date})")
        logger.info(f"Batch size: {batch_size}")
        logger.info(f"Dry run: {dry_run}")
        logger.info(f"Redis key prefix: {redis_key_prefix}")
    
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
        """Establish database and Redis connections."""
        # Database connection
        if self.db_type == 'postgresql':
            self.connection = psycopg2.connect(self.database_url)
            # Register bytea adapter to handle conversions
            psycopg2.extensions.register_type(psycopg2.extensions.UNICODE)
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
        
        # Redis connection
        try:
            self.redis_client = redis.from_url(self.redis_url, decode_responses=True)
            # Test connection
            self.redis_client.ping()
            logger.info("Connected to Redis")
        except Exception as e:
            logger.error(f"Failed to connect to Redis: {e}")
            raise
    
    def disconnect(self):
        """Close database and Redis connections."""
        if self.connection:
            self.connection.close()
            logger.info("Database connection closed")
        if self.redis_client:
            self.redis_client.close()
            logger.info("Redis connection closed")
    
    def _encode_to_bytea(self, text_value: str) -> bytes:
        """Convert text to bytea format for PostgreSQL."""
        return text_value.encode('utf-8')
    
    def _decode_from_bytea(self, bytea_value) -> str:
        """Convert bytea to text format."""
        if isinstance(bytea_value, memoryview):
            return bytes(bytea_value).decode('utf-8')
        elif isinstance(bytea_value, bytes):
            return bytea_value.decode('utf-8')
        else:
            return str(bytea_value)
    
    def check_run_active_in_redis(self, run_id: str) -> bool:
        """Check if a run has any active state in Redis."""
        if not self.redis_client:
            return False
        
        try:
            # Check global pause-run mapping first (most reliable)
            pause_run_key = f"{{{self.redis_key_prefix}}}:pr:{run_id}"
            if self.redis_client.exists(pause_run_key):
                logger.debug(f"Run {run_id} has active pauses in Redis")
                return True
            
            # Check for workspace-specific metadata
            pattern = f"{{{self.redis_key_prefix}:*}}:metadata:{run_id}"
            for key in self.redis_client.scan_iter(match=pattern, count=10):
                if key:
                    logger.debug(f"Run {run_id} has active metadata in Redis: {key}")
                    return True
            
            # Check for workspace-specific stack
            pattern = f"{{{self.redis_key_prefix}:*}}:stack:{run_id}"
            for key in self.redis_client.scan_iter(match=pattern, count=10):
                if key:
                    logger.debug(f"Run {run_id} has active stack in Redis: {key}")
                    return True
            
            return False
            
        except Exception as e:
            logger.warning(f"Error checking Redis state for run {run_id}: {e}")
            # Be conservative - assume it's active if we can't check
            return True
    
    def check_run_has_active_pauses(self, run_id: str) -> bool:
        """Check if a run has any active pauses waiting for it."""
        if not self.redis_client:
            return False
        
        try:
            # Check pause-run mapping
            pause_run_key = f"{{{self.redis_key_prefix}}}:pr:{run_id}"
            pause_ids = self.redis_client.smembers(pause_run_key)
            
            if pause_ids:
                # Verify at least one pause still exists
                for pause_id in pause_ids:
                    pause_key = f"{{{self.redis_key_prefix}}}:pauses:{pause_id}"
                    if self.redis_client.exists(pause_key):
                        logger.debug(f"Run {run_id} has active pause {pause_id}")
                        return True
            
            return False
            
        except Exception as e:
            logger.warning(f"Error checking pauses for run {run_id}: {e}")
            # Be conservative
            return True
    
    def filter_safe_to_delete_incomplete_runs(self, run_ids: List[Tuple]) -> List[bytes]:
        """Filter incomplete run IDs to only include those with no active Redis state (abandoned)."""
        if not run_ids:
            return []
        
        safe_run_ids = []
        
        for run_data in run_ids:
            # Extract run_id (might be tuple from fetchall)
            if isinstance(run_data, tuple):
                run_id_bytes = run_data[0]
            else:
                run_id_bytes = run_data
            
            # Convert bytea to string for Redis lookup
            run_id_str = self._decode_from_bytea(run_id_bytes)
            
            # For incomplete runs, presence of Redis state means it's still active
            if self.check_run_active_in_redis(run_id_str) or self.check_run_has_active_pauses(run_id_str):
                logger.debug(f"Skipping incomplete run {run_id_str} - has active Redis state")
                continue
            
            # No Redis state for an incomplete run means it's abandoned
            safe_run_ids.append(run_id_bytes)
        
        logger.info(f"Filtered {len(run_ids)} incomplete runs to {len(safe_run_ids)} abandoned runs safe to delete")
        return safe_run_ids
    
    def get_function_runs_to_delete(self) -> List[bytes]:
        """
        Get function run IDs to delete using improved logic:
        1. Completed runs (with function_finishes) older than cutoff - these are safe to delete
        2. Incomplete runs (no function_finishes) older than 2x cutoff - check Redis state for these
        """
        cursor = self.connection.cursor()
        completed_runs = []
        incomplete_runs = []
        
        try:
            if self.db_type == 'postgresql':
                # Get completed runs older than retention
                completed_query = """
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
                    ORDER BY ff.run_id
                    LIMIT %s
                """
                cursor.execute(completed_query, (self.cutoff_date, self.cutoff_date, self.batch_size))
                completed_runs = [row[0] for row in cursor.fetchall()]
                
                # Get incomplete runs older than 2x retention
                if len(completed_runs) < self.batch_size:
                    incomplete_query = """
                        SELECT fr.run_id
                        FROM function_runs fr
                        WHERE fr.run_started_at < %s
                          AND NOT EXISTS (
                              SELECT 1 FROM function_finishes ff
                              WHERE ff.run_id = fr.run_id
                          )
                        ORDER BY fr.run_id
                        LIMIT %s
                    """
                    cursor.execute(incomplete_query, (self.double_cutoff_date, self.batch_size - len(completed_runs)))
                    incomplete_runs = cursor.fetchall()
            else:
                # SQLite version (unchanged)
                completed_query = """
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
                    ORDER BY ff.run_id
                    LIMIT ?
                """
                cursor.execute(completed_query, (self.cutoff_date, self.cutoff_date, self.batch_size))
                completed_runs = [row[0] for row in cursor.fetchall()]
                
                if len(completed_runs) < self.batch_size:
                    incomplete_query = """
                        SELECT fr.run_id
                        FROM function_runs fr
                        WHERE fr.run_started_at < ?
                          AND NOT EXISTS (
                              SELECT 1 FROM function_finishes ff
                              WHERE ff.run_id = fr.run_id
                          )
                        ORDER BY fr.run_id
                        LIMIT ?
                    """
                    cursor.execute(incomplete_query, (self.double_cutoff_date, self.batch_size - len(completed_runs)))
                    incomplete_runs = cursor.fetchall()
            
            # Completed runs are safe to delete (Redis state already cleaned by Inngest)
            safe_run_ids = completed_runs
            
            # For incomplete runs, we need to check Redis state
            if incomplete_runs:
                logger.info(f"Checking Redis state for {len(incomplete_runs)} incomplete runs")
                safe_incomplete = self.filter_safe_to_delete_incomplete_runs(incomplete_runs)
                safe_run_ids.extend(safe_incomplete)
            
            logger.info(f"Found {len(completed_runs)} completed runs and {len(safe_run_ids) - len(completed_runs)} abandoned incomplete runs safe to delete")
            
        except Exception as e:
            logger.error(f"Error finding function runs to delete: {e}")
            raise
        finally:
            cursor.close()
        
        return safe_run_ids
    
    def clean_orphaned_redis_state(self) -> Dict[str, int]:
        """Clean Redis state for runs that no longer exist in the database."""
        stats = {
            'redis_metadata': 0,
            'redis_stack': 0,
            'redis_pauses': 0
        }
        
        if self.dry_run:
            logger.info("[DRY RUN] Would clean orphaned Redis state")
            return stats
        
        if not self.redis_client:
            logger.warning("No Redis connection - skipping Redis cleanup")
            return stats
        
        try:
            # Find run IDs in Redis
            redis_run_ids = set()
            
            # Scan for pause-run mappings (global)
            for key in self.redis_client.scan_iter(match=f"{{{self.redis_key_prefix}}}:pr:*", count=100):
                run_id = key.split(':')[-1]
                redis_run_ids.add(run_id)
            
            # Scan for workspace-specific metadata
            for key in self.redis_client.scan_iter(match=f"{{{self.redis_key_prefix}:*}}:metadata:*", count=100):
                run_id = key.split(':')[-1]
                redis_run_ids.add(run_id)
            
            if redis_run_ids:
                logger.info(f"Found {len(redis_run_ids)} runs in Redis, checking database...")
                
                # Check which runs exist in database
                cursor = self.connection.cursor()
                try:
                    if self.db_type == 'postgresql':
                        # Convert string run IDs to bytea for comparison
                        run_id_list = list(redis_run_ids)
                        placeholders = ','.join(['%s::bytea' for _ in run_id_list])
                        query = f"SELECT encode(run_id, 'escape') FROM function_runs WHERE run_id IN ({placeholders})"
                        cursor.execute(query, run_id_list)
                    else:
                        placeholders = ','.join(['?' for _ in redis_run_ids])
                        cursor.execute(
                            f"SELECT run_id FROM function_runs WHERE run_id IN ({placeholders})",
                            list(redis_run_ids)
                        )
                    
                    existing_runs = {row[0] for row in cursor.fetchall()}
                    orphaned_runs = redis_run_ids - existing_runs
                    
                    if orphaned_runs:
                        logger.info(f"Found {len(orphaned_runs)} orphaned runs in Redis")
                        
                        # Clean orphaned Redis state
                        for run_id in orphaned_runs:
                            # Delete pause-run mapping
                            if self.redis_client.delete(f"{{{self.redis_key_prefix}}}:pr:{run_id}"):
                                stats['redis_pauses'] += 1
                            
                            # Delete workspace-specific keys (scan for them)
                            for pattern in [f"{{{self.redis_key_prefix}:*}}:metadata:{run_id}",
                                          f"{{{self.redis_key_prefix}:*}}:stack:{run_id}"]:
                                for key in self.redis_client.scan_iter(match=pattern, count=10):
                                    if self.redis_client.delete(key):
                                        if 'metadata' in key:
                                            stats['redis_metadata'] += 1
                                        else:
                                            stats['redis_stack'] += 1
                    
                finally:
                    cursor.close()
            
        except Exception as e:
            logger.error(f"Error cleaning orphaned Redis state: {e}")
        
        return stats
    
    def clean_function_data(self) -> Dict[str, int]:
        """Clean function-related data with Redis awareness."""
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
        
        # Clean based on run IDs (with Redis filtering)
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
        
        return stats
    
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
    
    def get_referenced_event_ids(self, cutoff: datetime) -> Set[bytes]:
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
    
    def clean_events(self) -> int:
        """Clean old events that are not referenced."""
        if self.dry_run:
            logger.info("[DRY RUN] Would clean events")
            return 0
        
        # Get referenced event IDs (returns bytea for PostgreSQL)
        referenced_events = self.get_referenced_event_ids(self.cutoff_date)
        
        cursor = self.connection.cursor()
        deleted = 0
        
        try:
            if self.db_type == 'postgresql':
                # Use a temporary table for better performance
                cursor.execute("CREATE TEMP TABLE temp_referenced_events (event_id BYTEA)")
                
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
                # Convert timestamp to milliseconds for comparison with bigint
                cutoff_ms = int(self.cutoff_date.timestamp() * 1000)
                cursor.execute("""
                    SELECT run_id FROM trace_runs
                    WHERE ended_at IS NOT NULL
                      AND ended_at < %s
                    LIMIT %s
                """, (cutoff_ms, self.batch_size))
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
        """Execute the full cleanup process with Redis awareness."""
        try:
            self.connect()
            
            logger.info("Starting Redis-aware cleanup process...")
            logger.info(f"Using Redis key patterns without :state suffix")
            
            # Track what needs more cleanup
            needs_more = {
                'function_data': True,
                'events': True,
                'traces': True,
                'redis_orphans': True
            }
            
            total_stats = {
                'function_runs': 0,
                'function_finishes': 0,
                'history': 0,
                'orphaned_history': 0,
                'orphaned_finishes': 0,
                'events': 0,
                'traces': 0,
                'trace_runs': 0,
                'redis_metadata': 0,
                'redis_stack': 0,
                'redis_pauses': 0
            }
            
            # Keep cleaning until nothing left to clean
            while any(needs_more.values()):
                
                # Clean function data (Redis-aware)
                if needs_more['function_data']:
                    logger.info("Cleaning function data (checking Redis state)...")
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
                
                # Clean orphaned Redis state
                if needs_more['redis_orphans'] and self.redis_client:
                    logger.info("Cleaning orphaned Redis state...")
                    stats = self.clean_orphaned_redis_state()
                    
                    total_stats['redis_metadata'] += stats['redis_metadata']
                    total_stats['redis_stack'] += stats['redis_stack']
                    total_stats['redis_pauses'] += stats['redis_pauses']
                    
                    # Only run once per cleanup session
                    needs_more['redis_orphans'] = False
                
                # Small delay between batches
                if any(needs_more.values()):
                    import time
                    time.sleep(0.5)
            
            # Print summary
            logger.info("\nCleanup Summary:")
            logger.info("-" * 40)
            logger.info("Database cleanup:")
            logger.info("  Completed runs cleaned (safe - Redis already cleared by Inngest)")
            for table in ['function_runs', 'function_finishes', 'history']:
                if total_stats[table] > 0:
                    logger.info(f"    {table}: {total_stats[table]:,} records deleted")
            
            logger.info("  Orphaned/abandoned data cleaned:")
            for table in ['orphaned_history', 'orphaned_finishes', 'events', 'traces', 'trace_runs']:
                if total_stats[table] > 0:
                    logger.info(f"    {table}: {total_stats[table]:,} records deleted")
            
            logger.info("\nRedis cleanup (orphaned state):")
            for key in ['redis_metadata', 'redis_stack', 'redis_pauses']:
                if total_stats[key] > 0:
                    logger.info(f"  {key}: {total_stats[key]:,} keys deleted")
            
            logger.info(f"\nTotal records deleted: {sum(total_stats.values()):,}")
            
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
    redis_url = os.getenv('INNGEST_REDIS_URL', 'redis://localhost:6379')
    retention_days = int(os.getenv('INNGEST_RETENTION_DAYS', '30'))
    dry_run = os.getenv('INNGEST_DRY_RUN', 'false').lower() in ('true', '1', 'yes')
    batch_size = int(os.getenv('INNGEST_BATCH_SIZE', '1000'))
    double_retention = float(os.getenv('INNGEST_DOUBLE_RETENTION', '2.0'))
    redis_key_prefix = os.getenv('INNGEST_REDIS_KEY_PREFIX', 'inngest')
    
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
    logger.info("Inngest Redis-Aware Cleanup Configuration:")
    logger.info(f"  Database URL: {database_url[:20]}...")  # Log partial URL for security
    logger.info(f"  Redis URL: {redis_url[:20]}...")
    logger.info(f"  Retention days: {retention_days}")
    logger.info(f"  Dry run: {dry_run}")
    logger.info(f"  Batch size: {batch_size}")
    logger.info(f"  Double retention multiplier: {double_retention}")
    logger.info(f"  Redis key prefix: {redis_key_prefix}")
    
    try:
        cleaner = RedisAwareInngestCleaner(
            database_url=database_url,
            redis_url=redis_url,
            retention_days=retention_days,
            dry_run=dry_run,
            batch_size=batch_size,
            double_retention_multiplier=double_retention,
            redis_key_prefix=redis_key_prefix
        )
        cleaner.run_cleanup()
        logger.info("Cleanup completed successfully")
    except Exception as e:
        logger.error(f"Cleanup failed: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()