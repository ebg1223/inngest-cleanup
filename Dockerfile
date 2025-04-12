FROM ghcr.io/astral-sh/uv:python3.13-bookworm

# Install dependencies
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

# Install supercronic for ARM architecture
ENV SUPERCRONIC_URL=https://github.com/aptible/supercronic/releases/download/v0.2.33/supercronic-linux-arm64 \
  SUPERCRONIC_SHA1SUM=e0f0c06ebc5627e43b25475711e694450489ab00 \
  SUPERCRONIC=supercronic-linux-arm64

RUN curl -fsSLO "$SUPERCRONIC_URL" \
  && echo "${SUPERCRONIC_SHA1SUM}  ${SUPERCRONIC}" | sha1sum -c - \
  && chmod +x "$SUPERCRONIC" \
  && mv "$SUPERCRONIC" "/usr/local/bin/${SUPERCRONIC}" \
  && ln -s "/usr/local/bin/${SUPERCRONIC}" /usr/local/bin/supercronic

# Set up working directory
WORKDIR /app

# Copy application files
COPY pyproject.toml /app/
COPY uv.lock /app/

RUN uv pip install --system -e .

# Create a status file to track job execution
RUN touch /app/last_success

# Create crontab file with placeholder for environment variable
# This will be replaced at container start time
COPY entrypoint.sh /app/
RUN chmod +x /app/entrypoint.sh

# Set default cron schedule (overridable via environment variable)
ENV CRON_SCHEDULE="0 * * * *"

COPY healthcheck.py /app/
RUN chmod +x /app/healthcheck.py

# Configure healthcheck
HEALTHCHECK --interval=5m --timeout=3s --start-period=5s --retries=3 \
  CMD python /app/healthcheck.py

COPY cleanupv2.py /app/
RUN chmod +x /app/cleanupv2.py

COPY cleanupv2-sqlite.py /app/
RUN chmod +x /app/cleanupv2-sqlite.py

# Use entrypoint script to set up dynamic cron schedule
ENTRYPOINT ["/app/entrypoint.sh"]