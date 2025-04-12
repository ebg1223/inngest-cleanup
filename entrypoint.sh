#!/bin/bash
set -e

# Print environment for debugging
echo "Current environment:"
echo "PATH=$PATH"
echo "CRON_SCHEDULE=$CRON_SCHEDULE"


# Export all environment variables to a file
printenv > /app/env.sh

# Use absolute paths for commands
echo "${CRON_SCHEDULE} . /app/env.sh && /usr/local/bin/python /app/cleanupv2-sqlite.py && date > /app/last_success" > /app/crontab

# Print the crontab file for debugging
echo "Contents of crontab file:"
cat /app/crontab

# Make sure script.py exists
if [ ! -f /app/cleanupv2-sqlite.py ]; then
  echo "ERROR: /app/cleanupv2-sqlite.py does not exist!"
  exit 1
fi

# Make sure script.py is executable
chmod +x /app/cleanupv2-sqlite.py


# Start supercronic with the generated crontab
exec /usr/local/bin/supercronic /app/crontab
