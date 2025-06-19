# Changes from v2 to v3

## Summary
v3 adds Redis state awareness to prevent deleting active function runs while properly handling completed runs.

## Critical Bug Fix
v2 had no Redis awareness, which could lead to:
- Deleting database records for active functions
- "run not found in state store" errors
- Functions failing to resume after invokes

## Technical Changes

### 1. New Files
- `cleanup_inngest_env_with_redis.py` - Redis-aware cleanup script

### 2. Modified Files

#### `pyproject.toml`
- Added `redis>=6.2.0` dependency
- Updated version to 1.1.0
- Updated description

#### `Dockerfile`
- Copies both cleanup scripts
- Installs dependencies from pyproject.toml
- New environment variables for Redis

#### `cleanup_cron.sh`
- Checks `USE_REDIS_AWARE_CLEANUP` flag
- Runs appropriate cleanup script

#### `docker-entrypoint.sh`
- Updated to support Redis-aware cleanup
- Exports Redis variables to cron environment

### 3. New Environment Variables
```bash
INNGEST_REDIS_URL=redis://localhost:6379
USE_REDIS_AWARE_CLEANUP=true
INNGEST_REDIS_KEY_PREFIX=inngest
```

## Logic Changes

### v2 Behavior
- Delete any run older than retention period
- No consideration of active state

### v3 Behavior
- **Completed runs**: Delete if older than retention (Redis already cleaned)
- **Incomplete runs**: Only delete if no Redis state AND older than 2x retention
- **Orphan cleanup**: Remove Redis state for non-existent database records

## Deployment Notes

1. v3 is backward compatible - set `USE_REDIS_AWARE_CLEANUP=false` for v2 behavior
2. No database schema changes required
3. Recommended to run with `INNGEST_DRY_RUN=true` first
4. Monitor logs for skipped deletions to tune retention periods