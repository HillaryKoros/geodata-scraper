#!/bin/bash
set -e

echo "Waiting for PostGIS..."
while ! python -c "
import psycopg2
psycopg2.connect(
    dbname='${DB_NAME:-geodata}',
    user='${DB_USER:-geodata}',
    password='${DB_PASSWORD:-geodata}',
    host='${DB_HOST:-db}',
    port='${DB_PORT:-5432}'
)" 2>/dev/null; do
    sleep 1
done
echo "PostGIS ready."

echo "Running migrations..."
python manage.py migrate --noinput

echo "Starting $@"
exec "$@"
