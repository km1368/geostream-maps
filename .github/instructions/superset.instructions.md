---
description: "Use when working on Apache Superset configuration, dashboard assets, ClickHouse connectivity, or initialization scripts."
applyTo: "superset/**"
---

- Superset provides analytics dashboards at port 8088; it connects to ClickHouse via `clickhouse-connect` (`clickhousedb://clickhouse-1:8123/default`).
- `superset_init.py` programmatically registers the ClickHouse data source and imports dashboard assets on startup; keep scripts idempotent across container restarts.
- Asset files under `dashboard-assets/` use the placeholder `CLICKHOUSE_DATABASE_UUID`, which `superset_init.py` dynamically replaces with the database instance UUID at import time.
- Never hardcode credentials in dashboard assets, configs, or scripts; read all secrets from environment variables (`SUPERSET_SECRET_KEY`, `SUPERSET_ADMIN_*`, `CLICKHOUSE_*`).
- The provisioned dataset `recent_ping_history` queries `default.tbl_gps_processed_aggregated` with a 6-hour interval matching ClickHouse's TTL.
- Keep Iranian container image and package mirrors intact in `superset/Dockerfile` (`docker.arvancloud.ir` and `mirror-pypi.runflare.com`).
- The service must attach to `mapnet` and depends on `clickhouse-1` being healthy before initialization runs.
- `superset-init.sh` and `superset_config.py` are bind-mounted, but assets and `superset_init.py` are copied into the image; rebuild the image when updating copied files or Python packages.