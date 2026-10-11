import os
from collections import defaultdict
from datetime import timedelta

import pendulum
import psycopg
from airflow.sdk import dag, task
from pyiceberg.catalog import load_catalog
from pyiceberg.expressions import (
    And,
    EqualTo,
    GreaterThanOrEqual,
    In,
    LessThanOrEqual,
)


@dag(
    dag_id="street_speed_prediction",
    schedule="*/2 * * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Tehran"),
    catchup=False,
    max_active_runs=1,
    tags=["traffic", "prediction"],
)
def street_speed_prediction():
    @task
    def update_predicted_speeds():
        now = pendulum.now("Asia/Tehran")
        cutoff = now - timedelta(minutes=5)
        cutoff_text = cutoff.naive().isoformat()
        now_text = now.naive().isoformat()

        catalog = load_catalog(
            os.getenv("POLARIS_CATALOG", "polaris"),
            type="rest",
            uri=os.environ["POLARIS_URI"],
            credential=os.environ["POLARIS_CREDENTIAL"],
            scope="PRINCIPAL_ROLE:ALL",
            warehouse=os.getenv("POLARIS_CATALOG", "polaris"),
            **{
                "s3.endpoint": os.environ["MINIO_ENDPOINT"],
                "s3.region": os.getenv("AWS_REGION", "us-east-1"),
                "s3.access-key-id": os.environ["MINIO_ACCESS_KEY"],
                "s3.secret-access-key": os.environ["MINIO_SECRET_KEY"],
                "s3.path-style-access": "true",
                # pyiceberg sends X-Iceberg-Access-Delegation: vended-credentials
                # by default; Polaris can't vend STS creds for MinIO
                # (stsUnavailable) and answers 400 "Credential vending was
                # requested ... no credentials are available". Ask for no
                # delegation and read with the static MinIO keys above,
                # like the Spark lakehouse job does.
                "header.X-Iceberg-Access-Delegation": "none",
            },
        )
        table = catalog.load_table(
            f"{os.getenv('POLARIS_NAMESPACE', 'telemetry')}."
            f"{os.getenv('POLARIS_TABLE', 'gps_processed')}"
        )

        hours = []
        hour_cursor = cutoff.start_of("hour")
        while hour_cursor <= now:
            hours.append(hour_cursor)
            hour_cursor += timedelta(hours=1)

        row_filter = And(
            And(
                In("date", {hour.format("YYYY-MM-DD") for hour in hours}),
                In("hour", {hour.hour for hour in hours}),
            ),
            And(
                And(
                    GreaterThanOrEqual("timestamp", cutoff_text),
                    LessThanOrEqual("timestamp", now_text),
                ),
                EqualTo("is_valid", True),
            ),
        )
        records = table.scan(
            row_filter=row_filter,
            selected_fields=("street_id", "speed_kmh"),
        ).to_arrow().to_pylist()

        totals = defaultdict(float)
        counts = defaultdict(int)
        for record in records:
            street_id = record["street_id"]
            speed = record["speed_kmh"]
            if street_id is not None and speed is not None:
                totals[street_id] += speed
                counts[street_id] += 1

        averages = [
            (street_id, totals[street_id] / count)
            for street_id, count in counts.items()
        ]
        with psycopg.connect(
            host=os.getenv("PREDICTION_PGHOST", "postgres-1"),
            port=os.getenv("PREDICTION_PGPORT", "5432"),
            dbname=os.environ["PREDICTION_PGDATABASE"],
            user=os.environ["PREDICTION_PGUSER"],
            password=os.environ["PREDICTION_PGPASSWORD"],
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute("UPDATE roads SET predicted_speed_kmh = NULL")
                if averages:
                    values_sql = ", ".join(["(%s, %s)"] * len(averages))
                    parameters = [value for average in averages for value in average]
                    cursor.execute(
                        "UPDATE roads AS r "
                        "SET predicted_speed_kmh = v.avg_speed "
                        f"FROM (VALUES {values_sql}) AS v(osm_id, avg_speed) "
                        "WHERE r.osm_id = v.osm_id",
                        parameters,
                    )

        return f"Updated rolling speed averages for {len(averages)} streets"

    update_predicted_speeds()


street_speed_prediction()