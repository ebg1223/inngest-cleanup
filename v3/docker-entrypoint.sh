#!/bin/bash
# Docker entrypoint script

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
    echo "$CLEANUP_SCHEDULE /app/cleanup_cron.sh >> /var/log/cron.log 2>&1" | crontab -u inngest -
}

# Function to run cleanup immediately
run_cleanup_now() {
    echo "Running cleanup immediately..."
    if [ "$USE_REDIS_AWARE_CLEANUP" = "true" ] && [ -n "$INNGEST_REDIS_URL" ]; then
        echo "Using Redis-aware cleanup"
        su - inngest -c "cd /app && /app/.venv/bin/python /app/cleanup_inngest_env_with_redis.py"
    else
        echo "Using standard cleanup"
        su - inngest -c "cd /app && /app/.venv/bin/python /app/cleanup_inngest_env.py"
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
        echo "Batch size: $INNGEST_BATCH_SIZE"
        echo "Double retention: $INNGEST_DOUBLE_RETENTION"
        echo "Redis URL: ${INNGEST_REDIS_URL:-not configured}"
        echo "Redis-aware cleanup: ${USE_REDIS_AWARE_CLEANUP:-false}"
        
        # Run cron and tail the log
        cron && tail -f /var/log/cron.log
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
    
    redis-check|check-redis)
        echo "Checking Redis connection and Inngest keys"
        if [ -z "$INNGEST_REDIS_URL" ]; then
            echo "ERROR: INNGEST_REDIS_URL environment variable is required for Redis check"
            exit 1
        fi
        python /app/test_redis_connection.py
        ;;
    
    redis-scan|scan-redis)
        echo "Scanning all Redis keys to find patterns"
        if [ -z "$INNGEST_REDIS_URL" ]; then
            echo "ERROR: INNGEST_REDIS_URL environment variable is required for Redis scan"
            exit 1
        fi
        python /app/scan_redis_keys.py
        ;;
    
    diagnose|diagnostic)
        echo "Running comprehensive diagnostics"
        validate_database_url
        if [ -z "$INNGEST_REDIS_URL" ]; then
            echo "ERROR: INNGEST_REDIS_URL environment variable is required for diagnostics"
            exit 1
        fi
        python /app/diagnostic_check.py
        ;;
    
    verify)
        echo "Verifying cleanup behavior"
        validate_database_url
        if [ -z "$INNGEST_REDIS_URL" ]; then
            echo "ERROR: INNGEST_REDIS_URL environment variable is required for verification"
            exit 1
        fi
        python /app/verify_cleanup.py
        ;;
    
    *)
        echo "Usage: $0 {cron|once|test|redis-check|redis-scan|diagnose|verify}"
        echo "  cron: Run on schedule (default)"
        echo "  once: Run once and exit"
        echo "  test: Run once in dry-run mode"
        echo "  redis-check: Test Redis connection and show Inngest keys"
        echo "  redis-scan: Scan all Redis keys to find patterns"
        echo "  diagnose: Run comprehensive diagnostics on PostgreSQL and Redis"
        echo "  verify: Verify what cleanup would do and why"
        exit 1
        ;;
esac