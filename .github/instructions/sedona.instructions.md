---
description: "Use when working on Apache Sedona spatial streaming jobs, PySpark Structured Streaming, PostGIS updates, or Iceberg lakehouse ingestion."
applyTo: "sedona/**"
---

- Use Apache Sedona SQL spatial functions and PySpark Structured Streaming; keep transformations distributed and avoid Pandas.
- Parse Kafka JSON from the `value` column and keep its schema aligned with generator payloads and downstream output.
- In Spark 4 `foreachBatch`, use the batch DataFrame API and expressions; callback temp views are not visible from the outer SparkSession.
- `streaming_job.py` owns Kafka enrichment and Postgres traffic updates; `lakehouse_job.py` owns Iceberg/Polaris ingestion. Keep their responsibilities separate.
- Airflow submits `street_speed_prediction.py` to this container's standalone Spark master/worker; keep `SPARK_MASTER_HOST` and `SPARK_LOCAL_HOSTNAME` set to `sedona-1` so remote clients and workers do not advertise `localhost`.
- Keep the Spark submit client in Airflow on the same Spark version as this image, and preserve the shared Iceberg jar volume used by the prediction job.
- Every streaming query needs a persistent checkpoint; preserve existing checkpoint paths and Kafka offset behavior when changing query setup.
- Traffic validity, matching radius, trigger interval, and speed thresholds are currently hardcoded in `streaming_job.py`; Compose environment values for these are not authoritative.
- The lakehouse sink writes all processed events to `polaris.telemetry.gps_processed`; keep its schema synchronized with the nine-field JSON emitted by the streaming job.
- Dependencies are baked into the image and supplied with `--jars`; do not switch to runtime `--packages`. Rebuild after changing the Dockerfile or jar list.
- `hadoop-aws` must exactly match the `hadoop-client-api` version bundled in the base image (3.4.1 for apache/sedona:1.9.1) — verify with: docker run --rm <image> ls /opt/spark/jars | grep hadoop-client-api
- `bundle-2.24.6` is the AWS SDK v2 bundle that `hadoop-aws 3.4.1`'s POM declares (software.amazon.awssdk:bundle).
- `iceberg-spark-runtime` must match Spark 4.0 / Scala 2.13; `iceberg-aws-bundle` provides S3FileIO for the Polaris/MinIO catalog.
- `hadoop-client-api` is already bundled in the base image, never download it.
- `commons-pool2` ships in the image too, but at an older version; the Kafka connector needs `2.12.0`, so we fetch our own copy.
