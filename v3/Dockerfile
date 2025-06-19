# Use Python 3.12 slim image
FROM python:3.12-slim

# Install system dependencies (for psycopg2 and cron)
RUN apt-get update && apt-get install -y \
    gcc \
    cron \
    && rm -rf /var/lib/apt/lists/*

# Install uv
RUN pip install uv

# Create non-root user
RUN useradd -m -s /bin/bash inngest

# Set working directory
WORKDIR /app

# Copy project files
COPY pyproject.toml cleanup_inngest_env.py cleanup_inngest_env_with_redis.py ./
COPY cleanup_cron.sh docker-entrypoint.sh ./

# Make scripts executable
RUN chmod +x cleanup_cron.sh docker-entrypoint.sh

# Install dependencies directly
RUN uv pip install --system psycopg2-binary>=2.9.0 redis>=6.2.0

# Change ownership of app directory
RUN chown -R inngest:inngest /app

# Create log file with proper permissions
RUN touch /var/log/cron.log && chown inngest:inngest /var/log/cron.log

# Environment variables with defaults
ENV INNGEST_DATABASE_URL=""
ENV INNGEST_REDIS_URL="redis://localhost:6379"
ENV INNGEST_RETENTION_DAYS="3"
ENV INNGEST_DRY_RUN="false"
ENV INNGEST_BATCH_SIZE="1000"
ENV INNGEST_DOUBLE_RETENTION="2.0"
ENV INNGEST_REDIS_KEY_PREFIX="inngest"
ENV USE_REDIS_AWARE_CLEANUP="true"
ENV CLEANUP_SCHEDULE="0 2 * * *"

# Use entrypoint script
ENTRYPOINT ["/app/docker-entrypoint.sh"]