#!/usr/bin/env python3
"""
Diagnostic script to check PostgreSQL and Redis data consistency for Inngest.
Helps verify cleanup script behavior and data relationships.
"""

import os
import sys
import psycopg2
import redis
from datetime import datetime, timedelta
from collections import defaultdict
import json
import binascii

# ULID conversion utilities
def ulid_to_bytes(ulid_str):
    """Convert a ULID string to bytes for PostgreSQL bytea comparison."""
    ALPHABET = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
    ulid_str = ulid_str.upper()
    value = 0
    for char in ulid_str:
        value = value * 32 + ALPHABET.index(char)
    return value.to_bytes(16, byteorder='big')

def hex_to_ulid(hex_str):
    """Convert hex string to ULID for Redis lookups."""
    ALPHABET = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
    value = int(hex_str, 16)
    if value == 0:
        return ALPHABET[0] * 26
    chars = []
    while value > 0 and len(chars) < 26:
        chars.append(ALPHABET[value % 32])
        value //= 32
    result = ''.join(reversed(chars))
    return ALPHABET[0] * (26 - len(result)) + result

def decode_bytea(bytea_value):
    """Convert bytea to hex string representation."""
    if bytea_value is None:
        return None
    if isinstance(bytea_value, memoryview):
        return bytes(bytea_value).hex()
    elif isinstance(bytea_value, bytes):
        return bytea_value.hex()
    return str(bytea_value)

def run_diagnostics():
    """Run comprehensive diagnostics on PostgreSQL and Redis data."""
    # Get connection info
    db_url = os.getenv('INNGEST_DATABASE_URL')
    redis_url = os.getenv('INNGEST_REDIS_URL', 'redis://localhost:6379')
    redis_prefix = os.getenv('INNGEST_REDIS_KEY_PREFIX', 'inngest')
    retention_days = int(os.getenv('INNGEST_RETENTION_DAYS', '30'))
    
    if not db_url:
        print("ERROR: INNGEST_DATABASE_URL required")
        return False
    
    print("=== Inngest Data Diagnostics ===")
    print(f"Database: {db_url[:30]}...")
    print(f"Redis: {redis_url}")
    print(f"Redis prefix: {redis_prefix}")
    print(f"Retention: {retention_days} days")
    print()
    
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(db_url)
        cursor = conn.cursor()
        
        # Connect to Redis
        r = redis.from_url(redis_url, decode_responses=True)
        r.ping()
        
        # Calculate cutoffs
        cutoff_date = datetime.now() - timedelta(days=retention_days)
        double_cutoff = datetime.now() - timedelta(days=retention_days * 2)
        
        print(f"Cutoff date: {cutoff_date}")
        print(f"Double cutoff: {double_cutoff}")
        print()
        
        # 1. Database Overview
        print("=== PostgreSQL Overview ===")
        
        # Total counts
        tables = ['function_runs', 'function_finishes', 'history', 'events', 'trace_runs']
        for table in tables:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            print(f"{table}: {count:,} total records")
        
        print()
        
        # 2. Function runs analysis
        print("=== Function Runs Analysis ===")
        
        # Completed vs incomplete runs
        cursor.execute("""
            SELECT 
                COUNT(DISTINCT fr.run_id) as total_runs,
                COUNT(DISTINCT ff.run_id) as completed_runs
            FROM function_runs fr
            LEFT JOIN function_finishes ff ON fr.run_id = ff.run_id
        """)
        total_runs, completed_runs = cursor.fetchone()
        incomplete_runs = total_runs - completed_runs
        
        print(f"Total runs: {total_runs:,}")
        print(f"Completed: {completed_runs:,}")
        print(f"Incomplete: {incomplete_runs:,}")
        print()
        
        # Age distribution
        print("Age distribution of runs:")
        cursor.execute("""
            SELECT 
                CASE 
                    WHEN fr.run_started_at > %s THEN 'Within retention'
                    WHEN fr.run_started_at > %s THEN 'Between 1x-2x retention'
                    ELSE 'Older than 2x retention'
                END as age_group,
                COUNT(*) as count,
                COUNT(ff.run_id) as completed
            FROM function_runs fr
            LEFT JOIN function_finishes ff ON fr.run_id = ff.run_id
            GROUP BY age_group
            ORDER BY age_group
        """, (cutoff_date, double_cutoff))
        
        for row in cursor.fetchall():
            age_group, count, completed = row
            incomplete = count - completed
            print(f"  {age_group}: {count:,} total ({completed:,} completed, {incomplete:,} incomplete)")
        
        print()
        
        # 3. Redis Overview
        print("=== Redis Overview ===")
        
        # Count different key types
        key_counts = defaultdict(int)
        patterns = {
            'pauses': f"{{{redis_prefix}}}:pauses:*",
            'pause_runs': f"{{{redis_prefix}}}:pr:*",
            'metadata': f"{{{redis_prefix}:*}}:metadata:*",
            'stack': f"{{{redis_prefix}:*}}:stack:*",
            'pause_keys': f"{{{redis_prefix}:*}}:pause-key:*"
        }
        
        for name, pattern in patterns.items():
            count = 0
            for _ in r.scan_iter(match=pattern, count=100):
                count += 1
            key_counts[name] = count
            print(f"{name}: {count:,} keys")
        
        print()
        
        # 4. Data Consistency Checks
        print("=== Data Consistency Analysis ===")
        
        # Get runs with Redis state
        redis_runs = set()
        
        # From pause-run mappings
        for key in r.scan_iter(match=f"{{{redis_prefix}}}:pr:*", count=100):
            run_id = key.split(':')[-1]
            redis_runs.add(run_id)
        
        # From metadata
        for key in r.scan_iter(match=f"{{{redis_prefix}:*}}:metadata:*", count=100):
            run_id = key.split(':')[-1]
            redis_runs.add(run_id)
        
        print(f"Unique runs in Redis: {len(redis_runs):,}")
        
        # Check how many exist in database - convert Redis run IDs to check
        if redis_runs:
            run_list = list(redis_runs)[:1000]  # Check first 1000
            
            # Convert ULID strings to bytea format for PostgreSQL comparison
            existing_count = 0
            for ulid in run_list:
                try:
                    # Convert ULID to hex for PostgreSQL query
                    ulid_bytes = ulid_to_bytes(ulid)
                    ulid_hex = ulid_bytes.hex()
                    
                    cursor.execute(
                        "SELECT 1 FROM function_runs WHERE encode(run_id, 'hex') = %s LIMIT 1",
                        (ulid_hex,)
                    )
                    if cursor.fetchone():
                        existing_count += 1
                except Exception as e:
                    # Log the error but continue
                    print(f"  Error checking ULID {ulid}: {e}")
                    pass
            
            orphaned = len(run_list) - existing_count
            
            print(f"  Checked {len(run_list)} Redis runs:")
            print(f"  - {existing_count} exist in database")
            print(f"  - {orphaned} orphaned in Redis")
        
        print()
        
        # 5. Cleanup Candidates
        print("=== Cleanup Candidates ===")
        
        # Completed runs older than retention
        cursor.execute("""
            SELECT COUNT(*)
            FROM function_finishes ff
            WHERE ff.created_at < %s
              AND NOT EXISTS (
                  SELECT 1
                  FROM function_runs fr
                  LEFT JOIN function_finishes ff2 ON fr.run_id = ff2.run_id
                  WHERE fr.original_run_id = ff.run_id
                    AND (ff2.run_id IS NULL OR ff2.created_at >= %s)
              )
        """, (cutoff_date, cutoff_date))
        
        completed_candidates = cursor.fetchone()[0]
        print(f"Completed runs eligible for cleanup: {completed_candidates:,}")
        
        # Incomplete runs older than 2x retention
        cursor.execute("""
            SELECT COUNT(*)
            FROM function_runs fr
            WHERE fr.run_started_at < %s
              AND NOT EXISTS (
                  SELECT 1 FROM function_finishes ff
                  WHERE ff.run_id = fr.run_id
              )
        """, (double_cutoff,))
        
        incomplete_candidates = cursor.fetchone()[0]
        print(f"Incomplete runs older than 2x retention: {incomplete_candidates:,}")
        
        # Sample some incomplete runs to check Redis
        if incomplete_candidates > 0:
            cursor.execute("""
                SELECT encode(run_id, 'hex')
                FROM function_runs fr
                WHERE fr.run_started_at < %s
                  AND NOT EXISTS (
                      SELECT 1 FROM function_finishes ff
                      WHERE ff.run_id = fr.run_id
                  )
                LIMIT 10
            """, (double_cutoff,))
            
            sample_runs = cursor.fetchall()
            active_count = 0
            
            for (run_id_hex,) in sample_runs:
                # Check if active in Redis
                if (r.exists(f"{{{redis_prefix}}}:pr:{run_id_hex}") or 
                    any(r.scan_iter(match=f"{{{redis_prefix}:*}}:metadata:{run_id_hex}", count=1))):
                    active_count += 1
            
            if sample_runs:
                print(f"  Sample of {len(sample_runs)} incomplete runs: {active_count} still active in Redis")
                estimated_active = int(incomplete_candidates * (active_count / len(sample_runs)))
                print(f"  Estimated ~{estimated_active:,} incomplete runs still active")
                print(f"  Estimated ~{incomplete_candidates - estimated_active:,} truly abandoned")
        
        print()
        
        # 6. Events and Traces
        print("=== Events and Traces ===")
        
        # Old events
        cursor.execute("""
            SELECT COUNT(*) FROM events
            WHERE received_at < %s
        """, (cutoff_date,))
        old_events = cursor.fetchone()[0]
        print(f"Events older than retention: {old_events:,}")
        
        # Old traces
        cutoff_ms = int(cutoff_date.timestamp() * 1000)
        cursor.execute("""
            SELECT COUNT(*) FROM trace_runs
            WHERE ended_at IS NOT NULL AND ended_at < %s
        """, (cutoff_ms,))
        old_traces = cursor.fetchone()[0]
        print(f"Trace runs older than retention: {old_traces:,}")
        
        print()
        
        # 7. Data Type Information
        print("=== Data Type Information ===")
        cursor.execute("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name IN ('events', 'trace_runs', 'function_runs', 'function_finishes')
            AND column_name IN ('internal_id', 'event_id', 'ended_at', 'run_id')
            ORDER BY table_name, column_name
        """)
        
        print("Column types:")
        for col_name, data_type in cursor.fetchall():
            print(f"  {col_name}: {data_type}")
        
        # 8. Sample Data
        print("\n=== Sample Data ===")
        
        # Show sample run IDs in different formats
        cursor.execute("""
            SELECT 
                run_id,
                encode(run_id, 'hex') as hex,
                encode(run_id, 'escape') as escape,
                octet_length(run_id) as bytes
            FROM function_runs 
            LIMIT 3
        """)
        
        print("Sample run_id formats:")
        for run_id, hex_val, escape_val, byte_len in cursor.fetchall():
            print(f"  Raw: {byte_len} bytes")
            print(f"  Hex: {hex_val}")
            print(f"  Escape: {escape_val}")
            print()
        
        print("\n=== Diagnostics Complete ===")
        
        cursor.close()
        conn.close()
        r.close()
        
        return True
        
    except Exception as e:
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = run_diagnostics()
    sys.exit(0 if success else 1)