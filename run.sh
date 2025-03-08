#!/bin/sh

ARGS=""

# Convert environment variables to command line arguments
if [ ! -z "$DB_URL" ]; then
  ARGS="$ARGS --db-url $DB_URL"
fi

if [ ! -z "$RETENTION_DAYS" ]; then
  ARGS="$ARGS --retention-days $RETENTION_DAYS"
fi

if [ ! -z "$BATCH_SIZE" ]; then
  ARGS="$ARGS --batch-size $BATCH_SIZE"
fi

if [ ! -z "$SLEEP_SECONDS" ]; then
  ARGS="$ARGS --sleep-seconds $SLEEP_SECONDS"
fi

if [ "$DRY_RUN" = "true" ]; then
  ARGS="$ARGS --dry-run"
fi

if [ ! -z "$MAX_RETRIES" ]; then
  ARGS="$ARGS --max-retries $MAX_RETRIES"
fi

if [ ! -z "$RETRY_DELAY" ]; then
  ARGS="$ARGS --retry-delay $RETRY_DELAY"
fi

exec python cleanup_events.py $ARGS
