FROM python:3.13-alpine

WORKDIR /app

# Install system dependencies
RUN apk add --no-cache \
  postgresql-libs \
  gcc \
  python3-dev \
  musl-dev \
  postgresql-dev

# Install uv
RUN pip install --no-cache-dir uv

# Copy dependency files
COPY pyproject.toml .
COPY uv.lock .

# Install dependencies
RUN uv pip install --no-cache --system .

# Copy the cleanup script
COPY cleanup_events.py .

# Default command
ENTRYPOINT ["python", "cleanup_events.py"]

