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

# Install dependencies to a local directory
RUN uv pip install --no-cache --system --target=/install .

# Second stage - final image
FROM python:3.13-alpine

# Only runtime dependencies needed here
RUN apk add --no-cache postgresql-libs

WORKDIR /app

# Copy installed packages from builder stage
COPY --from=builder /install /usr/local/lib/python3.13/site-packages

# Copy application files
COPY run.sh .
RUN chmod +x run.sh
COPY cleanup_events.py .

# Default command
ENTRYPOINT ["/app/run.sh"]
