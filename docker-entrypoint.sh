#!/bin/sh

set -e

# Check if both HOST and PORT were extracted successfully
if [ -n "$DATABASE_HOST" ] && [ -n "$DATABASE_PORT" ]; then
    echo "INFO: Waiting for Postgres in $DATABASE_HOST:$DATABASE_PORT"
else
    echo "ERROR: DATABASE_HOST and DATABASE_PORT is not set"
    exit 1
fi

echo "INFO: Waiting for Postgres to start..."
while ! nc -z ${DATABASE_HOST} ${DATABASE_PORT}; do sleep 0.1; done
echo "INFO: Postgres is up"

exec "$@"
