#!/bin/sh
# set -e

# migrate internal superset db 
superset db upgrade
superset fab create-admin \
    --username "${SUPERSET_ADMIN_USERNAME}" \
    --firstname "${SUPERSET_ADMIN_FIRST_NAME}" \
    --lastname "${SUPERSET_ADMIN_LAST_NAME}" \
    --email "${SUPERSET_ADMIN_EMAIL}" \
    --password "${SUPERSET_ADMIN_PASSWORD}"
superset init

# connect to clickhouse and import chards, datasets, and dashboards
python /app/superset_init.py

# start superset webserver 
exec /app/docker/entrypoints/run-server.sh
