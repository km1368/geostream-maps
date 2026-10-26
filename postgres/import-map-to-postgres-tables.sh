#!/bin/sh
set -eu

PBF=/data/tehran.osm.pbf

if [ ! -f "$PBF" ] && [ -f /data/tehran.pbf ]; then
    PBF=/data/tehran.pbf
fi

if [ ! -f "$PBF" ]; then
    echo "Missing file: $PBF"
    exit 1
fi

echo "Waiting for PostgreSQL at $PGHOST:$PGPORT to be ready..."
retries=60
while ! nc -z -w 2 "$PGHOST" "$PGPORT"; do
    retries=$((retries - 1))
    if [ "$retries" -le 0 ]; then
        echo "Error: Timed out waiting for PostgreSQL at $PGHOST:$PGPORT"
        exit 1
    fi
    echo "PostgreSQL is not ready yet; waiting..."
    sleep 2
done
echo "PostgreSQL is accepting connections on $PGHOST:$PGPORT."

echo "Importing $PBF into PostgreSQL..."

max_attempts=3
attempt=1
while [ "$attempt" -le "$max_attempts" ]; do
    if osm2pgsql \
        --create \
        --slim \
        --output flex \
        --style /scripts/road-styles-for-import.lua \
        --database "$PGDATABASE" \
        --host "$PGHOST" \
        --port "$PGPORT" \
        --username "$PGUSER" \
        --number-processes 4 \
        "$PBF"; then
        break
    fi

    if [ "$attempt" -eq "$max_attempts" ]; then
        echo "osm2pgsql failed after $max_attempts attempts."
        exit 1
    fi

    echo "osm2pgsql failed on attempt $attempt; retrying in 3 seconds..."
    sleep 3
    attempt=$((attempt + 1))
done

echo "osm2pgsql done. Post-import SQL is applied by the 'post_import' service."

echo "Import complete."