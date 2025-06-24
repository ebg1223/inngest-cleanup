#!/bin/sh
# Simplified Docker entrypoint script

# Function to validate database URL
validate_database_url() {
    if [ -z "$INNGEST_DATABASE_URL" ]; then
        echo "ERROR: INNGEST_DATABASE_URL environment variable is required"
        exit 1
    fi
}

# Function to update cron schedule
update_cron_schedule() {
    echo "Setting cron schedule to: ${CLEANUP_SCHEDULE:-0 2 * * *}"
    echo "$CLEANUP_SCHEDULE /app/cleanup_cron.sh >> /var/log/cron.log 2>&1" | crontab -
}

# Function to run cleanup immediately
run_cleanup_now() {
    echo "Running cleanup immediately..."
    cd /app
    if [ "$USE_REDIS_AWARE_CLEANUP" = "true" ] && [ -n "$INNGEST_REDIS_URL" ]; then
        echo "Using Redis-aware cleanup"
        uv run cleanup_inngest_env_with_redis.py
    else
        echo "Using standard cleanup"
        uv run cleanup_inngest_env.py
    fi
}

# Main entrypoint logic
case "${1:-cron}" in
    cron)
        echo "Starting Inngest cleanup in cron mode"
        validate_database_url
        update_cron_schedule
        
        # Export environment variables for cron
        printenv | grep -E '^INNGEST_|^USE_REDIS_AWARE_CLEANUP' > /etc/environment
        
        # Start cron in foreground
        echo "Cron schedule: ${CLEANUP_SCHEDULE:-0 2 * * *}"
        echo "Retention days: $INNGEST_RETENTION_DAYS"
        echo "Dry run: $INNGEST_DRY_RUN"
        
        # Create log file if it doesn't exist
        touch /var/log/cron.log
        
        # Run cron and tail the log
        crond -f -L /var/log/cron.log &
        tail -f /var/log/cron.log
        ;;
    
    once)
        echo "Running cleanup once and exiting"
        validate_database_url
        run_cleanup_now
        ;;
    
    test)
        echo "Running cleanup in test mode (dry run)"
        validate_database_url
        export INNGEST_DRY_RUN="true"
        run_cleanup_now
        ;;
    
    *)
        echo "Usage: $0 {cron|once|test}"
        echo "  cron: Run on schedule (default)"
        echo "  once: Run once and exit"
        echo "  test: Run once in dry-run mode"
        exit 1
        ;;
esac