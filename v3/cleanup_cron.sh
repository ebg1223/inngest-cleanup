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

# Run the cleanup script
# Check if Redis-aware cleanup is enabled
if [ "$USE_REDIS_AWARE_CLEANUP" = "true" ] && [ -n "$INNGEST_REDIS_URL" ]; then
    echo "Running Redis-aware cleanup script"
    python /app/cleanup_inngest_env_with_redis.py
else
    echo "Running standard cleanup script"
    python /app/cleanup_inngest_env.py
fi

# Log completion
echo "Cleanup completed at $(date)"
echo ""