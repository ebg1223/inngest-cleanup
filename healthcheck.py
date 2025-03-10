import os
import sys
import time
from datetime import datetime

def main():
    # Path to the last success file
    last_success_file = "/app/last_success"
    
    # Check if the last success file exists
    if not os.path.exists(last_success_file):
        print("Error: Last success file not found")
        sys.exit(1)
    
    # Check when the file was last modified
    try:
        with open(last_success_file, 'r') as f:
            last_success_time = f.read().strip()
        
        # For more sophisticated checks, parse the timestamp and verify it's recent enough
        # This is a simple file existence check, but you can make it more robust
        
        # You could also add DB connectivity check here if appropriate
        
        print("Healthcheck passed")
        sys.exit(0)
    except Exception as e:
        print(f"Healthcheck failed: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()