#!/usr/bin/env python3
"""
Test Redis connection for Inngest cleanup script.
"""

import os
import sys
import redis
from urllib.parse import urlparse

def test_redis_connection():
    """Test Redis connection and check for Inngest keys."""
    redis_url = os.getenv('INNGEST_REDIS_URL', 'redis://localhost:6379')
    redis_key_prefix = os.getenv('INNGEST_REDIS_KEY_PREFIX', 'inngest')
    
    print(f"Testing Redis connection to: {redis_url}")
    print(f"Using key prefix: {redis_key_prefix}")
    
    try:
        # Connect to Redis
        client = redis.from_url(redis_url, decode_responses=True)
        
        # Test connection
        client.ping()
        print("✓ Redis connection successful")
        
        # Check for Inngest keys
        patterns = [
            f"{{{redis_key_prefix}:state}}:metadata:*",
            f"{{{redis_key_prefix}:state}}:pauses:*",
            f"{{{redis_key_prefix}:state}}:pr:*"
        ]
        
        total_keys = 0
        for pattern in patterns:
            keys = list(client.scan_iter(match=pattern, count=100))
            count = len(keys)
            total_keys += count
            print(f"  Found {count} keys matching {pattern}")
            
            # Show sample keys
            if keys and count <= 5:
                for key in keys[:5]:
                    print(f"    - {key}")
            elif keys:
                for key in keys[:3]:
                    print(f"    - {key}")
                print(f"    ... and {count - 3} more")
        
        print(f"\nTotal Inngest keys found: {total_keys}")
        
        # Get Redis info
        info = client.info()
        print(f"\nRedis server info:")
        print(f"  Version: {info.get('redis_version', 'unknown')}")
        print(f"  Used memory: {info.get('used_memory_human', 'unknown')}")
        print(f"  Connected clients: {info.get('connected_clients', 'unknown')}")
        
        return True
        
    except redis.ConnectionError as e:
        print(f"✗ Failed to connect to Redis: {e}")
        return False
    except Exception as e:
        print(f"✗ Error: {e}")
        return False

if __name__ == "__main__":
    success = test_redis_connection()
    sys.exit(0 if success else 1)