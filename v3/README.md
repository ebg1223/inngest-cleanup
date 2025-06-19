# Inngest Cleanup v3 - Redis-Aware Database Cleanup

Version 3 adds Redis state awareness to prevent cleaning up active function runs.

## Key Features

- **Redis-aware cleanup**: Checks Redis state before deleting database records
- **Smart run detection**: Differentiates between completed and incomplete runs
- **Prevents "run not found" errors**: Won't delete runs with active state
- **Backward compatible**: Falls back to v2 behavior if Redis not configured

## What's New in v3

### 1. Redis State Checking
- Completed runs (with `function_finishes`): Deleted based on age (Redis already cleaned by Inngest)
- Incomplete runs: Only deleted if no Redis state AND older than 2x retention period

### 2. New Environment Variables
- `INNGEST_REDIS_URL`: Redis connection URL (default: `redis://localhost:6379`)
- `USE_REDIS_AWARE_CLEANUP`: Enable Redis checking (default: `true`)
- `INNGEST_REDIS_KEY_PREFIX`: Redis key prefix (default: `inngest`)

### 3. Orphaned State Cleanup
- Cleans Redis state for runs that no longer exist in the database
- Prevents Redis memory leaks from failed database operations

## Configuration

### Basic Usage
```bash
docker run -e INNGEST_DATABASE_URL=postgresql://user:pass@host/db \
           -e INNGEST_REDIS_URL=redis://redis:6379 \
           ghcr.io/your-org/inngest-cleaner:latest
```

### Disable Redis Checking
```bash
docker run -e INNGEST_DATABASE_URL=postgresql://user:pass@host/db \
           -e USE_REDIS_AWARE_CLEANUP=false \
           ghcr.io/your-org/inngest-cleaner:latest
```

## How It Works

1. **Query completed runs** older than retention period
   - These are safe to delete (Redis state already cleaned)
   
2. **Query incomplete runs** older than 2x retention period
   - Check each for active Redis state
   - Only delete if no state found (abandoned runs)

3. **Clean orphaned Redis state**
   - Find Redis keys with no corresponding database records
   - Remove to prevent memory leaks

## Migration from v2

1. Ensure `INNGEST_REDIS_URL` points to your Inngest Redis instance
2. Set `INNGEST_REDIS_KEY_PREFIX` to match your Inngest configuration
3. Deploy v3 - it will automatically use Redis-aware cleanup

To test first:
```bash
docker run -e INNGEST_DRY_RUN=true ... # See what would be deleted
```

## Diagnostic Tools

### Redis Connection Testing

Test your Redis connection and find Inngest keys:

```bash
# Basic Redis check
docker run --rm \
  -e INNGEST_REDIS_URL=redis://your-redis:6379 \
  -e INNGEST_REDIS_KEY_PREFIX=estate \
  ghcr.io/your-org/inngest-cleaner:latest redis-check

# Scan all Redis keys to find patterns
docker run --rm \
  -e INNGEST_REDIS_URL=redis://your-redis:6379 \
  ghcr.io/your-org/inngest-cleaner:latest redis-scan
```

### Comprehensive Diagnostics

Run full diagnostics on PostgreSQL and Redis data:

```bash
docker run --rm \
  -e INNGEST_DATABASE_URL=postgresql://... \
  -e INNGEST_REDIS_URL=redis://your-redis:6379 \
  -e INNGEST_REDIS_KEY_PREFIX=estate \
  ghcr.io/your-org/inngest-cleaner:latest diagnose
```

Shows:
- Database record counts and age distribution
- Redis key counts by type
- Data consistency analysis
- Cleanup candidates
- Column type information

### Verify Cleanup Behavior

See exactly what would be cleaned and why:

```bash
docker run --rm \
  -e INNGEST_DATABASE_URL=postgresql://... \
  -e INNGEST_REDIS_URL=redis://your-redis:6379 \
  -e INNGEST_REDIS_KEY_PREFIX=estate \
  -e INNGEST_RETENTION_DAYS=3 \
  ghcr.io/your-org/inngest-cleaner:latest verify
```

Shows:
- Sample runs that would be deleted
- Why each run would be kept or deleted
- Redis state for incomplete runs
- Potential issues detected

## Troubleshooting

### Still getting "run not found" errors?
- Run `redis-check` to verify connection: `docker run ... redis-check`
- Verify `INNGEST_REDIS_URL` is correct
- Check `INNGEST_REDIS_KEY_PREFIX` matches your Inngest setup
- Ensure cleanup container can reach Redis

### Cleanup not deleting anything?
- Normal for completed runs - v3 correctly handles them
- Check logs for "X completed runs and Y abandoned incomplete runs"
- Verify retention settings are appropriate