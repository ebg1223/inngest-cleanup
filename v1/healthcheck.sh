#!/bin/sh
# Healthcheck script for event cleaner service
# This script tests if the healthcheck server is responding on the configured port

# Use the HEALTHCHECK_PORT environment variable with a fallback to 8080
PORT=${HEALTHCHECK_PORT:-8080}

# Try to connect to the healthcheck server
curl -f -s -m 5 http://localhost:${PORT}/health || exit 1

# If we get here, the service is healthy
exit 0 