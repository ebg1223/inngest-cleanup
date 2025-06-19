#!/usr/bin/env python3
"""
Verify cleanup script behavior - check what would be cleaned and why.
"""

import os
import sys
import psycopg2
import redis
from datetime import datetime, timedelta

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

def verify_cleanup():
    """Verify what the cleanup script would do."""
    # Get connection info
    db_url = os.getenv('INNGEST_DATABASE_URL')
    redis_url = os.getenv('INNGEST_REDIS_URL', 'redis://localhost:6379')
    redis_prefix = os.getenv('INNGEST_REDIS_KEY_PREFIX', 'inngest')
    retention_days = int(os.getenv('INNGEST_RETENTION_DAYS', '30'))
    
    if not db_url:
        print("ERROR: INNGEST_DATABASE_URL required")
        return False
    
    print("=== Cleanup Verification ===")
    print(f"Retention: {retention_days} days")
    print()
    
    try:
        # Connect
        conn = psycopg2.connect(db_url)
        cursor = conn.cursor()
        r = redis.from_url(redis_url, decode_responses=True)
        
        # Calculate cutoffs
        cutoff_date = datetime.now() - timedelta(days=retention_days)
        double_cutoff = datetime.now() - timedelta(days=retention_days * 2)
        
        # 1. Check completed runs that would be deleted
        print("=== Completed Runs (Safe to Delete) ===")
        cursor.execute("""
            SELECT 
                encode(ff.run_id, 'hex') as run_id_hex,
                ff.created_at,
                fr.function_id
            FROM function_finishes ff
            JOIN function_runs fr ON fr.run_id = ff.run_id
            WHERE ff.created_at < %s
              AND NOT EXISTS (
                  SELECT 1
                  FROM function_runs fr2
                  LEFT JOIN function_finishes ff2 ON fr2.run_id = ff2.run_id
                  WHERE fr2.original_run_id = ff.run_id
                    AND (ff2.run_id IS NULL OR ff2.created_at >= %s)
              )
            ORDER BY ff.created_at DESC
            LIMIT 5
        """, (cutoff_date, cutoff_date))
        
        completed = cursor.fetchall()
        if completed:
            print(f"Sample of {len(completed)} completed runs to be deleted:")
            for run_id_hex, created_at, func_id in completed:
                age_days = (datetime.now() - created_at).days
                print(f"  {run_id_hex}: {func_id} (completed {age_days} days ago)")
        else:
            print("No completed runs to delete")
        
        print()
        
        # 2. Check incomplete runs
        print("=== Incomplete Runs (Need Redis Check) ===")
        cursor.execute("""
            SELECT 
                encode(fr.run_id, 'hex') as run_id_hex,
                fr.run_started_at,
                fr.function_id
            FROM function_runs fr
            WHERE fr.run_started_at < %s
              AND NOT EXISTS (
                  SELECT 1 FROM function_finishes ff
                  WHERE ff.run_id = fr.run_id
              )
            ORDER BY fr.run_started_at DESC
            LIMIT 10
        """, (double_cutoff,))
        
        incomplete = cursor.fetchall()
        if incomplete:
            print(f"Checking {len(incomplete)} incomplete runs older than {retention_days*2} days:")
            
            would_delete = []
            would_keep = []
            
            for run_id_hex, started_at, func_id in incomplete:
                age_days = (datetime.now() - started_at).days
                
                # Convert hex to ULID for Redis lookup
                try:
                    run_id_ulid = hex_to_ulid(run_id_hex)
                except Exception as e:
                    print(f"Error converting {run_id_hex} to ULID: {e}")
                    run_id_ulid = run_id_hex  # Fallback to hex
                
                # Check Redis state using ULID representation
                has_pauses = r.exists(f"{{{redis_prefix}}}:pr:{run_id_ulid}")
                has_metadata = any(r.scan_iter(match=f"{{{redis_prefix}:*}}:metadata:{run_id_ulid}", count=1))
                has_stack = any(r.scan_iter(match=f"{{{redis_prefix}:*}}:stack:{run_id_ulid}", count=1))
                
                has_redis_state = has_pauses or has_metadata or has_stack
                
                status = []
                if has_pauses:
                    status.append("pauses")
                if has_metadata:
                    status.append("metadata")
                if has_stack:
                    status.append("stack")
                
                if has_redis_state:
                    would_keep.append((run_id_hex, func_id, age_days, status))
                else:
                    would_delete.append((run_id_hex, func_id, age_days))
            
            if would_keep:
                print(f"\n  Would KEEP {len(would_keep)} runs (have Redis state):")
                for run_id, func_id, age_days, status in would_keep[:5]:
                    status_str = ", ".join(status)
                    print(f"    {run_id}: {func_id} ({age_days} days old) - Redis: {status_str}")
            
            if would_delete:
                print(f"\n  Would DELETE {len(would_delete)} runs (no Redis state):")
                for run_id, func_id, age_days in would_delete[:5]:
                    print(f"    {run_id}: {func_id} ({age_days} days old) - No Redis state")
        else:
            print("No incomplete runs older than 2x retention")
        
        print()
        
        # 3. Check for potential issues
        print("=== Potential Issues ===")
        
        # Check for runs with pauses but no metadata
        orphaned_pauses = 0
        for key in r.scan_iter(match=f"{{{redis_prefix}}}:pr:*", count=100):
            run_id = key.split(':')[-1]
            if not any(r.scan_iter(match=f"{{{redis_prefix}:*}}:metadata:{run_id}", count=1)):
                orphaned_pauses += 1
        
        if orphaned_pauses > 0:
            print(f"WARNING: {orphaned_pauses} runs have pauses but no metadata")
        
        # Check for very old incomplete runs with Redis state
        cursor.execute("""
            SELECT COUNT(*)
            FROM function_runs fr
            WHERE fr.run_started_at < %s
              AND NOT EXISTS (
                  SELECT 1 FROM function_finishes ff
                  WHERE ff.run_id = fr.run_id
              )
        """, (datetime.now() - timedelta(days=30),))
        
        very_old_incomplete = cursor.fetchone()[0]
        if very_old_incomplete > 0:
            print(f"INFO: {very_old_incomplete} incomplete runs older than 30 days")
        
        # Check Redis key format consistency
        print("\n=== Redis Key Format Check ===")
        sample_pr_keys = list(r.scan_iter(match=f"{{{redis_prefix}}}:pr:*", count=5))
        if sample_pr_keys:
            print("Sample pause-run keys:")
            for key in sample_pr_keys[:3]:
                run_id = key.split(':')[-1]
                print(f"  Key: {key}")
                print(f"  Run ID: {run_id} (length: {len(run_id)})")
                
                # Check if it looks like hex
                try:
                    bytes.fromhex(run_id)
                    print(f"  Format: Valid hex")
                except:
                    print(f"  Format: Not hex - possibly ULID or other format")
        
        print("\n=== Verification Complete ===")
        
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
    success = verify_cleanup()
    sys.exit(0 if success else 1)