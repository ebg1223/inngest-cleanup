#!/usr/bin/env python3
"""
Scan Redis to find all keys and identify potential Inngest patterns.
"""

import os
import sys
import redis
from collections import defaultdict

def scan_all_keys():
    """Scan Redis for all keys and group by pattern."""
    redis_url = os.getenv('INNGEST_REDIS_URL', 'redis://localhost:6379')
    
    print(f"Scanning all keys in Redis: {redis_url}")
    
    try:
        # Connect to Redis
        client = redis.from_url(redis_url, decode_responses=True)
        
        # Test connection
        client.ping()
        print("✓ Redis connection successful\n")
        
        # Get database info
        info = client.info()
        db_info = client.info('keyspace')
        current_db = client.connection_pool.connection_kwargs.get('db', 0)
        
        print(f"Connected to database: {current_db}")
        for db_name, db_stats in db_info.items():
            print(f"  {db_name}: {db_stats}")
        print()
        
        # Scan all keys
        print("Scanning keys...")
        all_keys = []
        pattern_counts = defaultdict(int)
        prefix_counts = defaultdict(int)
        
        # Scan with small count for better progress feedback
        for key in client.scan_iter(match='*', count=100):
            all_keys.append(key)
            
            # Count patterns
            if ':' in key:
                parts = key.split(':')
                # First part (prefix)
                prefix_counts[parts[0]] += 1
                # First two parts (prefix:type)
                if len(parts) > 1:
                    pattern = f"{parts[0]}:{parts[1]}"
                    pattern_counts[pattern] += 1
        
        print(f"\nTotal keys found: {len(all_keys)}")
        
        if all_keys:
            print("\nKey prefixes:")
            for prefix, count in sorted(prefix_counts.items(), key=lambda x: -x[1]):
                print(f"  {prefix}: {count} keys")
            
            print("\nKey patterns (prefix:type):")
            for pattern, count in sorted(pattern_counts.items(), key=lambda x: -x[1])[:20]:
                print(f"  {pattern}: {count} keys")
            
            # Look for potential Inngest patterns
            print("\nPotential Inngest patterns:")
            inngest_patterns = [
                'state', 'queue', 'events', 'function', 'runs', 'pauses',
                'metadata', 'history', 'stack', 'actions', 'invoke', 'signal'
            ]
            
            found_inngest = False
            for key in all_keys[:1000]:  # Check first 1000 keys
                for pattern in inngest_patterns:
                    if pattern in key.lower():
                        print(f"  Found: {key}")
                        found_inngest = True
                        break
                if found_inngest:
                    break
            
            if not found_inngest:
                print("  No obvious Inngest patterns found")
            
            # Show sample keys
            print("\nSample keys (first 10):")
            for key in all_keys[:10]:
                key_type = client.type(key)
                ttl = client.ttl(key)
                ttl_str = f"TTL: {ttl}s" if ttl > 0 else "No TTL" if ttl == -1 else "Expired"
                print(f"  {key} (type: {key_type}, {ttl_str})")
        
        else:
            print("\nNo keys found in the current database.")
            print("\nChecking other databases...")
            
            # Try to check other databases
            for db_num in range(16):  # Redis default max databases
                try:
                    test_client = redis.from_url(redis_url.replace('/0', f'/{db_num}'), decode_responses=True)
                    test_client.ping()
                    db_size = test_client.dbsize()
                    if db_size > 0:
                        print(f"  Database {db_num}: {db_size} keys")
                except:
                    pass
        
        return True
        
    except redis.ConnectionError as e:
        print(f"✗ Failed to connect to Redis: {e}")
        return False
    except Exception as e:
        print(f"✗ Error: {e}")
        return False

if __name__ == "__main__":
    success = scan_all_keys()
    sys.exit(0 if success else 1)