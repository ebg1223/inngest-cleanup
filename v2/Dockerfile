# Use Python 3.12 slim image
FROM python:3.12-slim AS builder

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Install uv
RUN pip install uv

# Set working directory
WORKDIR /app

# Copy project files
COPY pyproject.toml .
COPY cleanup_inngest_env.py .

# Create virtual environment and install dependencies
RUN uv venv
RUN . .venv/bin/activate && uv pip install -e .

# Runtime stage
FROM python:3.12-slim

# Install cron and other runtime dependencies
RUN apt-get update && apt-get install -y \
    cron \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN useradd -m -s /bin/bash inngest

# Set working directory
WORKDIR /app

# Copy virtual environment and script from builder
COPY --from=builder /app/.venv /app/.venv
COPY --from=builder /app/cleanup_inngest_env.py .

# Copy cron script and entrypoint
COPY cleanup_cron.sh .
COPY docker-entrypoint.sh .

# Make scripts executable
RUN chmod +x cleanup_cron.sh docker-entrypoint.sh

# Change ownership of app directory
RUN chown -R inngest:inngest /app

# Create log file with proper permissions
RUN touch /var/log/cron.log && chown inngest:inngest /var/log/cron.log

# Environment variables with defaults
ENV INNGEST_DATABASE_URL=""
ENV INNGEST_RETENTION_DAYS="3"
ENV INNGEST_DRY_RUN="false"
ENV INNGEST_BATCH_SIZE="1000"
ENV INNGEST_DOUBLE_RETENTION="2.0"
ENV CLEANUP_SCHEDULE="0 2 * * *"

# Note: We'll set up the cron job in the entrypoint script
# to allow dynamic schedule configuration

# Use entrypoint script
ENTRYPOINT ["/app/docker-entrypoint.sh"]
