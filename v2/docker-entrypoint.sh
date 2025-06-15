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
    su - inngest -c "cd /app && python /app/cleanup_inngest_env.py"
}

# Main entrypoint logic
case "${1:-cron}" in
    cron)
        echo "Starting Inngest cleanup in cron mode"
        validate_database_url
        update_cron_schedule
        
        # Export environment variables for cron
        printenv | grep -E '^INNGEST_' > /etc/environment
        
        # Start cron in foreground
        echo "Cron schedule: ${CLEANUP_SCHEDULE:-0 2 * * *}"
        echo "Retention days: $INNGEST_RETENTION_DAYS"
        echo "Dry run: $INNGEST_DRY_RUN"
        echo "Batch size: $INNGEST_BATCH_SIZE"
        echo "Double retention: $INNGEST_DOUBLE_RETENTION"
        
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
    
    *)
        echo "Usage: $0 {cron|once|test}"
        echo "  cron: Run on schedule (default)"
        echo "  once: Run once and exit"
        echo "  test: Run once in dry-run mode"
        exit 1
        ;;
esac