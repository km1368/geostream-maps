---
description: "Use when working on PostGIS database tables, OSM road imports, spatial indexes, or live traffic state schema."
applyTo: "postgres/**"
---

- Postgres/PostGIS stores OSM roads and current traffic state; use PostgreSQL and PostGIS SQL.
- Preserve `roads.osm_id` as the stable street key used by streaming, prediction, tiles, and historical lookups.
- Create GiST indexes for geometry columns and appropriate B-tree indexes for frequently filtered or joined attributes.
- Keep `roads.predicted_speed_kmh` compatible with the Airflow prediction writer and Sedona traffic-level calculation.
- OSM import and traffic-column setup are separate one-shot Compose services; follow the README initialization order before relying on the `roads` table.
- Keep geometry serialization and SRIDs consistent with the existing OSM import and Sedona spatial joins.
- Do not place credentials in SQL or source files; use the existing Compose environment and `.env` configuration.