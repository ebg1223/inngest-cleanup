# Database Cleanup Tool

This tool performs automated cleanup of old events and orphaned records in a PostgreSQL database.

## Environment Variables

The script is configured using the following environment variables:

### Database Connection

- `DB_HOST`: Database hostname (default: `localhost`)
- `DB_PORT`: Database port (default: `5432`)
- `DB_NAME`: Database name (default: `dbname`)
- `DB_USER`: Database username (default: `username`)
- `DB_PASSWORD`: Database password (default: `password`)

### Cleanup Configuration

- `CLEANUP_RETENTION_HOURS`: Retention period in hours (default: `720` = 30 days)
- `CLEANUP_BATCH_SIZE`: Number of records per batch (default: `1000`)
- `CLEANUP_MAX_RUNTIME_SECONDS`: Maximum runtime in seconds (default: `300` = 5 minutes)
- `CLEANUP_MODE`: Cleanup mode - `age`, `orphaned`, or `all` (default: `all`)

### Scheduling

- `CRON_SCHEDULE`: Cron schedule for regular execution (default: `0 * * * *` = hourly)

## Docker Deployment

The tool is deployed as a Docker container that runs on the specified cron schedule.

```bash
docker run \
  -e DB_HOST=your_host \
  -e DB_PORT=5432 \
  -e DB_NAME=your_db \
  -e DB_USER=your_user \
  -e DB_PASSWORD=your_password \
  -e CLEANUP_RETENTION_HOURS=720 \
  -e CLEANUP_BATCH_SIZE=1000 \
  -e CLEANUP_MAX_RUNTIME_SECONDS=300 \
  -e CLEANUP_MODE=all \
  -e CRON_SCHEDULE="0 * * * *" \
  your-repository/db-cleanup-tool
```

## Running Locally

To run the script locally:

```bash
export DB_HOST=localhost
export DB_PORT=5432
export DB_NAME=your_db
export DB_USER=your_user
export DB_PASSWORD=your_password
export CLEANUP_RETENTION_HOURS=720
export CLEANUP_BATCH_SIZE=1000
export CLEANUP_MAX_RUNTIME_SECONDS=300
export CLEANUP_MODE=all

python cleanup_events.py
```

## Modes

- `age`: Cleans up events and related records older than `CLEANUP_RETENTION_HOURS`
- `orphaned`: Cleans up orphaned function_runs and function_finishes with no associated event
- `all`: Runs both cleanup modes sequentially
