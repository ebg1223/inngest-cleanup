#!/usr/bin/env python3
"""
ULID to binary conversion utilities for Redis-PostgreSQL matching.
"""

def ulid_to_bytes(ulid_str):
    """
    Convert a ULID string to bytes for PostgreSQL bytea comparison.
    
    ULID format: 26 characters in Crockford's Base32
    Example: 01JY1JJ822BNZGF3DAHM0HVKDT
    
    Returns 16 bytes that can be compared with PostgreSQL bytea columns.
    """
    # Crockford's Base32 alphabet (excludes I, L, O, U to avoid confusion)
    ALPHABET = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
    
    # Convert to uppercase for consistency
    ulid_str = ulid_str.upper()
    
    # Convert base32 to integer
    value = 0
    for char in ulid_str:
        value = value * 32 + ALPHABET.index(char)
    
    # Convert to 16 bytes (128 bits)
    return value.to_bytes(16, byteorder='big')

def bytes_to_hex(byte_data):
    """Convert bytes to hex string for comparison."""
    return byte_data.hex()

def ulid_to_hex(ulid_str):
    """Convert ULID string directly to hex for PostgreSQL queries."""
    return bytes_to_hex(ulid_to_bytes(ulid_str))

# Test the conversion
if __name__ == "__main__":
    # Test with sample ULID from Redis
    test_ulid = "01JY1JJ822BNZGF3DAHM0HVKDT"
    print(f"ULID: {test_ulid}")
    print(f"Hex:  {ulid_to_hex(test_ulid)}")
    print(f"Length: {len(ulid_to_hex(test_ulid))} chars")