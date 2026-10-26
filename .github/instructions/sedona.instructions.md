---
description: "Use when working on Apache Sedona spatial streaming jobs, PySpark Structured Streaming, PostGIS updates, or Iceberg lakehouse ingestion."
applyTo: "sedona/**"
---

- Use Apache Sedona SQL spatial functions and PySpark Structured Streaming; keep transformations distributed and avoid Pandas.
- Parse Kafka JSON from the `value` column and keep its schema aligned with generator payloads and downstream output.
- In Spark 4 `foreachBatch`, use the batch DataFrame API and expressions; callback temp views are not visible from the outer SparkSession.
- `streaming_job.py` owns Kafka enrichment and Postgres traffic updates; `lakehouse_job.py` owns Iceberg/Polaris ingestion. Keep their responsibilities separate.
- Every streaming query needs a persistent checkpoint; preserve existing checkpoint paths and Kafka offset behavior when changing query setup.
- Traffic validity, matching radius, trigger interval, and speed thresholds are currently hardcoded in `streaming_job.py`; Compose environment values for these are not authoritative.
- The lakehouse sink writes all processed events to `polaris.telemetry.gps_processed`; keep its schema synchronized with the nine-field JSON emitted by the streaming job.
- Dependencies are baked into the image and supplied with `--jars`; do not switch to runtime `--packages`. Rebuild after changing the Dockerfile or jar list.
