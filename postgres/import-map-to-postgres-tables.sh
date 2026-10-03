#!/bin/sh
set -eu

PBF=/data/tehran.osm.pbf

if [ ! -f "$PBF" ]; then
    echo "Missing file: $PBF"
    exit 1
fi

echo "Importing $PBF into PostgreSQL..."

osm2pgsql \
    --create \
    --slim \
    --output flex \
    --style /scripts/road-styles-for-import.lua \
    --database "$PGDATABASE" \
    --host "$PGHOST" \
    --port "$PGPORT" \
    --username "$PGUSER" \
    --number-processes 4 \
    "$PBF"

echo "osm2pgsql done. Post-import SQL is applied by the 'post_import' service."

echo "Import complete."