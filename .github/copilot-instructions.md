# Real-Time Traffic Map

This project streams simulated Tehran GPS telemetry into a live street-traffic map and a historical lakehouse. The stack is orchestrated with Docker Compose.

## System at a glance

- GPS events flow through Redpanda and Spark/Sedona; processed events update PostGIS traffic state and are also stored in Iceberg on MinIO through Polaris.
- Martin serves PostGIS vector tiles to the MapLibre web app; ClickHouse provides recent street history, while Airflow writes rolling speed predictions to PostGIS.
- Preserve the configured Iranian image mirrors when adding or changing container images.

## Working in this repository

- Use the README for setup and operational procedures; use the matching file under `.github/instructions/` for service-specific implementation constraints and gotchas.
- Keep secrets in `.env` or the existing secret configuration, never in source files or committed instructions.

