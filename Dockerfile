FROM python:3.13-alpine AS builder

# Install build dependencies
RUN apk add --no-cache \
  postgresql-dev \
  gcc \
  python3-dev \
  musl-dev

# Install uv
RUN pip install --no-cache-dir uv

# Copy dependency files
WORKDIR /build
COPY pyproject.toml .
COPY uv.lock .

# The key change: Replace psycopg2-binary with psycopg2 in your pyproject.toml
# You don't need to do this here if you've already updated the file locally

# Install dependencies to a local directory
RUN uv pip install --no-cache --system --target=/install .

# Second stage - final image
FROM python:3.13-alpine

# Runtime dependencies and curl for healthchecks
RUN apk add --no-cache postgresql-libs curl

WORKDIR /app

# Copy installed packages from builder stage
COPY --from=builder /install /usr/local/lib/python3.13/site-packages

# Copy application files
COPY run.sh .
RUN chmod +x run.sh
COPY cleanup_events.py .

# Set a default healthcheck port if not specified at runtime
ENV HEALTHCHECK_PORT=8080

# Create a healthcheck script that will read the current environment variable value
RUN echo '#!/bin/sh\ncurl -f http://localhost:${HEALTHCHECK_PORT}/health || exit 1' > /app/healthcheck.sh \
  && chmod +x /app/healthcheck.sh

# Add healthcheck using the script
HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 \
  CMD /app/healthcheck.sh

# Default command
ENTRYPOINT ["/app/run.sh"]

