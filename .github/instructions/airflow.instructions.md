---
description: "Use when developing or updating Airflow DAGs, TaskFlow tasks, PyIceberg lakehouse queries, or street speed prediction workflows."
applyTo: "airflow/**"
---

- Use TaskFlow for Python tasks and provider operators for delegated execution, including `SparkSubmitOperator` for prediction.
- Airflow orchestrates work: keep heavy transformations in Spark, Postgres, ClickHouse, or the lakehouse.
- The prediction DAG submits `sedona/street_speed_prediction.py` through `SparkSubmitOperator` to the standalone Spark master in `sedona-1`; the job averages the last five minutes of valid telemetry by street and updates `roads.predicted_speed_kmh` every two minutes.
- Configure external systems through environment-backed Airflow configuration; never hardcode credentials or connection secrets.
- For direct PyIceberg REST reads, set `header.X-Iceberg-Access-Delegation` to `none` and use static MinIO S3 credentials; Polaris cannot vend STS credentials for MinIO.
- PyIceberg catalog settings must use warehouse name `polaris`, full `http://` S3 endpoint, path-style access, and `PRINCIPAL_ROLE:ALL` scope.
- With PyIceberg 0.11.1, omit `row_filter` when there is no filter; explicitly passing `None` raises a `TypeError`.
- DAG files are bind-mounted and reload on scheduler parse; rebuild the Airflow image only after changing its Dockerfile or requirements.
