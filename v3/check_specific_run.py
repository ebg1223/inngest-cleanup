#!/usr/bin/env python3
"""
Check if a specific run ID exists in PostgreSQL and Redis.
Usage: python check_specific_run.py <run_id>
"""

import sys
import os
from datetime import datetime
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import RealDictCursor
import redis
import json
from typing import Optional, Dict, Any
import ulid

# Load environment variables
load_dotenv()

# Database connection parameters
DB_PARAMS = {
    'host': os.getenv('PGHOST', 'localhost'),
    'port': os.getenv('PGPORT', '5432'),
    'database': os.getenv('PGDATABASE', 'postgres'),
    'user': os.getenv('PGUSER', 'postgres'),
    'password': os.getenv('PGPASSWORD', '')
}

# Redis connection parameters
REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.getenv('REDIS_PORT', '6379'))
REDIS_DB = int(os.getenv('REDIS_DB', '0'))
REDIS_PASSWORD = os.getenv('REDIS_PASSWORD', None)


def ulid_to_bytea(ulid_str: str) -> bytes:
    """Convert ULID string to bytea format for PostgreSQL."""
    try:
        ulid_obj = ulid.from_str(ulid_str)
        return ulid_obj.bytes
    except Exception as e:
        print(f"Error converting ULID: {e}")
        return None


def bytea_to_ulid(bytea_value: bytes) -> str:
    """Convert bytea to ULID string."""
    try:
        ulid_obj = ulid.from_bytes(bytea_value)
        return str(ulid_obj)
    except Exception as e:
        print(f"Error converting bytea to ULID: {e}")
        return None


def check_postgresql(run_id: str) -> Dict[str, Any]:
    """Check if run exists in PostgreSQL."""
    result = {
        'found': False,
        'function_runs': [],
        'function_finishes': [],
        'related_info': {}
    }
    
    bytea_id = ulid_to_bytea(run_id)
    if not bytea_id:
        result['error'] = 'Failed to convert ULID to bytea'
        return result
    
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        # Check function_runs table
        cur.execute("""
            SELECT 
                id,
                run_started_at,
                function_id,
                run_id,
                status,
                event_id,
                batch_id,
                cron_schedule,
                original_run_id,
                workflow_id
            FROM function_runs
            WHERE id = %s
        """, (bytea_id,))
        
        rows = cur.fetchall()
        if rows:
            result['found'] = True
            for row in rows:
                # Convert bytea fields to ULID strings
                row_dict = dict(row)
                for field in ['id', 'run_id', 'event_id', 'batch_id', 'original_run_id', 'workflow_id']:
                    if row_dict.get(field):
                        row_dict[field] = bytea_to_ulid(row_dict[field])
                
                # Format timestamp
                if row_dict.get('run_started_at'):
                    row_dict['run_started_at'] = row_dict['run_started_at'].isoformat()
                
                result['function_runs'].append(row_dict)
        
        # Check function_finishes table
        cur.execute("""
            SELECT 
                function_run_id,
                function_id,
                status,
                output,
                completed_step_count,
                created_at
            FROM function_finishes
            WHERE function_run_id = %s
        """, (bytea_id,))
        
        finish_rows = cur.fetchall()
        for row in finish_rows:
            row_dict = dict(row)
            if row_dict.get('function_run_id'):
                row_dict['function_run_id'] = bytea_to_ulid(row_dict['function_run_id'])
            if row_dict.get('created_at'):
                row_dict['created_at'] = row_dict['created_at'].isoformat()
            result['function_finishes'].append(row_dict)
        
        # Get related batch information if batch_id exists
        if result['function_runs'] and result['function_runs'][0].get('batch_id'):
            batch_id_str = result['function_runs'][0]['batch_id']
            batch_bytea = ulid_to_bytea(batch_id_str)
            
            cur.execute("""
                SELECT 
                    COUNT(*) as total_runs,
                    COUNT(CASE WHEN status = 0 THEN 1 END) as running,
                    COUNT(CASE WHEN status = 1 THEN 1 END) as completed,
                    COUNT(CASE WHEN status = 2 THEN 1 END) as failed,
                    COUNT(CASE WHEN status = 3 THEN 1 END) as cancelled,
                    MIN(run_started_at) as earliest_start,
                    MAX(run_started_at) as latest_start
                FROM function_runs
                WHERE batch_id = %s
            """, (batch_bytea,))
            
            batch_info = cur.fetchone()
            if batch_info:
                batch_dict = dict(batch_info)
                for field in ['earliest_start', 'latest_start']:
                    if batch_dict.get(field):
                        batch_dict[field] = batch_dict[field].isoformat()
                result['related_info']['batch_stats'] = batch_dict
        
        cur.close()
        conn.close()
        
    except Exception as e:
        result['error'] = f"PostgreSQL error: {str(e)}"
    
    return result


def check_redis(run_id: str) -> Dict[str, Any]:
    """Check if run exists in Redis."""
    result = {
        'found': False,
        'keys': [],
        'data': {}
    }
    
    try:
        # Connect to Redis
        r = redis.Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            db=REDIS_DB,
            password=REDIS_PASSWORD,
            decode_responses=True
        )
        
        # Check for keys containing the run ID
        pattern = f"*{run_id}*"
        keys = list(r.scan_iter(match=pattern, count=1000))
        
        if keys:
            result['found'] = True
            result['keys'] = keys
            
            # Get data for each key
            for key in keys[:10]:  # Limit to first 10 keys
                key_type = r.type(key)
                
                if key_type == 'string':
                    value = r.get(key)
                    try:
                        # Try to parse as JSON
                        result['data'][key] = json.loads(value)
                    except:
                        result['data'][key] = value
                elif key_type == 'hash':
                    result['data'][key] = r.hgetall(key)
                elif key_type == 'list':
                    result['data'][key] = r.lrange(key, 0, -1)
                elif key_type == 'set':
                    result['data'][key] = list(r.smembers(key))
                elif key_type == 'zset':
                    result['data'][key] = r.zrange(key, 0, -1, withscores=True)
                else:
                    result['data'][key] = f"Type: {key_type}"
        
    except Exception as e:
        result['error'] = f"Redis error: {str(e)}"
    
    return result


def format_status(status: int) -> str:
    """Convert status code to string."""
    status_map = {
        0: "Running",
        1: "Completed",
        2: "Failed",
        3: "Cancelled"
    }
    return status_map.get(status, f"Unknown ({status})")


def main():
    if len(sys.argv) != 2:
        print("Usage: python check_specific_run.py <run_id>")
        sys.exit(1)
    
    run_id = sys.argv[1]
    print(f"\nChecking for run ID: {run_id}")
    print("=" * 80)
    
    # Check PostgreSQL
    print("\n📊 PostgreSQL Check:")
    print("-" * 40)
    pg_result = check_postgresql(run_id)
    
    if pg_result.get('error'):
        print(f"❌ Error: {pg_result['error']}")
    elif pg_result['found']:
        print(f"✅ Found in PostgreSQL!")
        
        if pg_result['function_runs']:
            print("\nFunction Run Details:")
            for run in pg_result['function_runs']:
                print(f"  - Started at: {run.get('run_started_at', 'N/A')}")
                print(f"  - Function ID: {run.get('function_id', 'N/A')}")
                print(f"  - Status: {format_status(run.get('status', -1))}")
                print(f"  - Event ID: {run.get('event_id', 'N/A')}")
                print(f"  - Batch ID: {run.get('batch_id', 'N/A')}")
                if run.get('workflow_id'):
                    print(f"  - Workflow ID: {run['workflow_id']}")
        
        if pg_result['function_finishes']:
            print("\nFunction Finish Details:")
            for finish in pg_result['function_finishes']:
                print(f"  - Status: {format_status(finish.get('status', -1))}")
                print(f"  - Created at: {finish.get('created_at', 'N/A')}")
                print(f"  - Completed steps: {finish.get('completed_step_count', 0)}")
                if finish.get('output'):
                    print(f"  - Has output: Yes")
        
        if pg_result.get('related_info', {}).get('batch_stats'):
            stats = pg_result['related_info']['batch_stats']
            print("\nBatch Statistics:")
            print(f"  - Total runs in batch: {stats['total_runs']}")
            print(f"  - Running: {stats['running']}")
            print(f"  - Completed: {stats['completed']}")
            print(f"  - Failed: {stats['failed']}")
            print(f"  - Cancelled: {stats['cancelled']}")
            print(f"  - Batch timespan: {stats['earliest_start']} to {stats['latest_start']}")
    else:
        print("❌ Not found in PostgreSQL")
    
    # Check Redis
    print("\n🔑 Redis Check:")
    print("-" * 40)
    redis_result = check_redis(run_id)
    
    if redis_result.get('error'):
        print(f"❌ Error: {redis_result['error']}")
    elif redis_result['found']:
        print(f"✅ Found in Redis!")
        print(f"Found {len(redis_result['keys'])} key(s) containing the run ID:")
        
        for key in redis_result['keys'][:10]:
            print(f"  - {key}")
        
        if len(redis_result['keys']) > 10:
            print(f"  ... and {len(redis_result['keys']) - 10} more keys")
        
        if redis_result['data']:
            print("\nKey Data (first 10 keys):")
            for key, data in redis_result['data'].items():
                print(f"\n  Key: {key}")
                if isinstance(data, dict):
                    for k, v in list(data.items())[:5]:
                        print(f"    {k}: {v}")
                    if len(data) > 5:
                        print(f"    ... and {len(data) - 5} more fields")
                else:
                    print(f"    {str(data)[:200]}{'...' if len(str(data)) > 200 else ''}")
    else:
        print("❌ Not found in Redis")
    
    # Summary
    print("\n" + "=" * 80)
    print("📋 Summary:")
    if pg_result['found'] and redis_result['found']:
        print("✅ Run exists in both PostgreSQL and Redis")
    elif pg_result['found'] and not redis_result['found']:
        print("⚠️  Run exists in PostgreSQL but NOT in Redis")
        print("   This might explain why Inngest can't find it - it may be looking in Redis first")
    elif not pg_result['found'] and redis_result['found']:
        print("⚠️  Run exists in Redis but NOT in PostgreSQL")
        print("   This suggests data inconsistency between stores")
    else:
        print("❌ Run not found in either PostgreSQL or Redis")
        print("   The run may have been deleted or the ID may be incorrect")


if __name__ == "__main__":
    main()