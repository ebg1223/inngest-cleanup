# Use official uv image
FROM ghcr.io/astral-sh/uv:python3.12-alpine

# Install cron
RUN apk add --no-cache dcron

# Set working directory
WORKDIR /app

# Copy project files
COPY pyproject.toml uv.lock* ./
COPY *.py ./
COPY cleanup_cron.sh docker-entrypoint.sh ./

# Install dependencies
RUN uv sync --frozen --no-dev

# Make scripts executable
RUN chmod +x cleanup_cron.sh docker-entrypoint.sh

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