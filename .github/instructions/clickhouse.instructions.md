---
description: "Use when working on ClickHouse schemas, MergeTree tables, Kafka streaming engines, materialized views, or telemetry aggregations."
applyTo: "clickhouse/**"
---

- Use ClickHouse SQL and design for OLAP; prefer appropriate `MergeTree` engines.
- The Kafka engine consumes Redpanda at the internal address `redpanda-1:29092`.
- Match Kafka input columns to Spark's JSON payload; use `DateTime64` for microsecond ISO-8601 timestamps.
- Keep the materialized view and aggregate table aligned with the `gps-processed` schema; the history table has a six-hour TTL.
- Aggregate-function state columns are binary states; query them with merge functions such as `avgMerge(...)` and `GROUP BY`, not `SELECT *`.
- ClickHouse init scripts run only on an empty data volume; rerun the idempotent init script manually when initializing an existing volume.
- Keep the `/clickhouse/` nginx proxy credentials aligned with the existing `.env` values; never commit credentials.
- Attach the service to `mapnet` so it can resolve Redpanda and other Compose services.
