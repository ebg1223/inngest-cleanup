#!/usr/bin/env python3
"""
Debug script to investigate Redis to PostgreSQL key matching.
"""

import os
import sys
import psycopg2
import redis
from datetime import datetime, timedelta

def decode_bytea(bytea_value):
    """Convert bytea to hex string representation."""
    if bytea_value is None:
        return None
    if isinstance(bytea_value, memoryview):
        return bytes(bytea_value).hex()
    elif isinstance(bytea_value, bytes):
        return bytea_value.hex()
    return str(bytea_value)

def debug_redis_pg_match():
    """Debug Redis to PostgreSQL run ID matching."""
    # Get connection info
    db_url = os.getenv('INNGEST_DATABASE_URL')
    redis_url = os.getenv('INNGEST_REDIS_URL', 'redis://localhost:6379')
    redis_prefix = os.getenv('INNGEST_REDIS_KEY_PREFIX', 'inngest')
    
    if not db_url:
        print("ERROR: INNGEST_DATABASE_URL required")
        return False
    
    print("=== Redis to PostgreSQL Matching Debug ===")
    print()
    
    try:
        # Connect
        conn = psycopg2.connect(db_url)
        cursor = conn.cursor()
        r = redis.from_url(redis_url, decode_responses=True)
        
        # 1. Get sample Redis runs
        print("=== Sample Redis Pause-Run Keys ===")
        pr_keys = list(r.scan_iter(match=f"{{{redis_prefix}}}:pr:*", count=5))
        
        if not pr_keys:
            print("No pause-run keys found in Redis")
            return True
        
        print(f"Found {len(pr_keys)} pause-run keys. Examining first 5:")
        
        for i, key in enumerate(pr_keys[:5]):
            print(f"\n{i+1}. Redis key: {key}")
            
            # Extract run ID from key
            parts = key.split(':')
            redis_run_id = parts[-1]
            print(f"   Extracted run ID: {redis_run_id}")
            print(f"   Length: {len(redis_run_id)} chars")
            
            # Check if it's hex
            try:
                bytes.fromhex(redis_run_id)
                print(f"   Format: Valid hex")
            except:
                print(f"   Format: Not hex (possibly ULID or UUID)")
            
            # Try different matching strategies
            print("   Checking PostgreSQL for matches:")
            
            # Strategy 1: Direct hex match (lowercase)
            cursor.execute(
                "SELECT 1 FROM function_runs WHERE encode(run_id, 'hex') = %s LIMIT 1",
                (redis_run_id.lower(),)
            )
            if cursor.fetchone():
                print("   ✓ Found with lowercase hex match")
                continue
            
            # Strategy 2: Uppercase hex match
            cursor.execute(
                "SELECT 1 FROM function_runs WHERE encode(run_id, 'hex') = %s LIMIT 1",
                (redis_run_id.upper(),)
            )
            if cursor.fetchone():
                print("   ✓ Found with uppercase hex match")
                continue
            
            # Strategy 3: If it looks like a UUID, try different formats
            if len(redis_run_id) == 36 and '-' in redis_run_id:
                # Remove dashes and try as hex
                uuid_hex = redis_run_id.replace('-', '')
                cursor.execute(
                    "SELECT 1 FROM function_runs WHERE encode(run_id, 'hex') = %s LIMIT 1",
                    (uuid_hex.lower(),)
                )
                if cursor.fetchone():
                    print("   ✓ Found with UUID hex match")
                    continue
            
            # Strategy 4: Check if Redis ID exists as-is in any text column
            cursor.execute(
                "SELECT 1 FROM function_runs WHERE function_id = %s OR run_id::text = %s LIMIT 1",
                (redis_run_id, redis_run_id)
            )
            if cursor.fetchone():
                print("   ✓ Found in text columns")
                continue
            
            print("   ✗ Not found in database")
            
            # Show what run_ids look like in the database for comparison
            cursor.execute(
                "SELECT encode(run_id, 'hex') FROM function_runs ORDER BY run_started_at DESC LIMIT 1"
            )
            sample = cursor.fetchone()
            if sample:
                print(f"   Sample DB run_id: {sample[0]}")
        
        print("\n=== Recent PostgreSQL Run IDs ===")
        cursor.execute("""
            SELECT 
                encode(run_id, 'hex') as hex,
                run_started_at,
                function_id
            FROM function_runs 
            ORDER BY run_started_at DESC 
            LIMIT 5
        """)
        
        print("Recent database runs:")
        for hex_id, started_at, func_id in cursor.fetchall():
            age = datetime.now() - started_at
            print(f"  {hex_id} - {func_id} ({age.days} days old)")
            
            # Check if this exists in Redis
            if r.exists(f"{{{redis_prefix}}}:pr:{hex_id}"):
                print(f"    ✓ Found in Redis with lowercase hex")
            elif r.exists(f"{{{redis_prefix}}}:pr:{hex_id.upper()}"):
                print(f"    ✓ Found in Redis with uppercase hex")
            else:
                print(f"    ✗ Not found in Redis")
        
        print("\n=== Key Format Analysis ===")
        
        # Check if we're dealing with workspace-specific patterns
        all_keys = list(r.scan_iter(match=f"{{{redis_prefix}}}:*", count=100))
        workspace_keys = [k for k in all_keys if ':ws:' in k or ':workspace:' in k]
        
        if workspace_keys:
            print(f"Found {len(workspace_keys)} workspace-specific keys")
            print("Sample workspace keys:")
            for key in workspace_keys[:3]:
                print(f"  {key}")
        
        # Analyze key patterns
        key_patterns = {}
        for key in all_keys[:100]:
            # Extract pattern (everything before the last segment)
            parts = key.split(':')
            if len(parts) > 1:
                pattern = ':'.join(parts[:-1]) + ':*'
                key_patterns[pattern] = key_patterns.get(pattern, 0) + 1
        
        print("\nKey patterns found:")
        for pattern, count in sorted(key_patterns.items(), key=lambda x: x[1], reverse=True)[:10]:
            print(f"  {pattern}: {count} keys")
        
        print("\n=== Debug Complete ===")
        
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
    success = debug_redis_pg_match()
    sys.exit(0 if success else 1)