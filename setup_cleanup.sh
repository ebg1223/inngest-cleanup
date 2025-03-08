#!/bin/bash
# Create and activate venv if it doesn't exist
if [ ! -d ".venv" ]; then
  uv venv
  source .venv/bin/activate
  uv pip install -r requirements.txt
else
  source .venv/bin/activate
fi

# Default values
: "${RETENTION_DAYS:=1}"
: "${BATCH_SIZE:=5000}"
: "${SLEEP_SECONDS:="0.1"}"
: "${MAX_RETRIES:=3}"
: "${RETRY_DELAY:=5}"

# Run the cleanup script
python cleanup_events.py \
  --db-url="${DB_URL}" \
  --retention-days="${RETENTION_DAYS}" \
  --batch-size="${BATCH_SIZE}" \
  --sleep-seconds="${SLEEP_SECONDS}" \
  --max-retries="${MAX_RETRIES}" \
  --retry-delay "${RETRY_DELAY}" \
  ${DRY_RUN:+--dry-run}
