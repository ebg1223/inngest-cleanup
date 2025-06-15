#!/bin/bash
# Cron script to run the Inngest cleanup

# Source environment variables from file (cron doesn't inherit them)
if [ -f /etc/environment ]; then
    . /etc/environment
fi

# Log start time
echo "========================================="
echo "Starting Inngest cleanup at $(date)"
echo "========================================="

# Change to app directory
cd /app

# Run the cleanup script with virtual environment
/app/.venv/bin/python /app/cleanup_inngest_env.py

# Log completion
echo "Cleanup completed at $(date)"
echo ""