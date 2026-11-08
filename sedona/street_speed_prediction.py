import os
from datetime import timedelta

import pendulum
import psycopg
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main():
    now = pendulum.now("Asia/Tehran")
    cutoff = now - timedelta(minutes=5)
    cutoff_text = cutoff.naive().isoformat()
    now_text = now.naive().isoformat()

    catalog_name = os.getenv("POLARIS_CATALOG", "polaris")
    namespace = os.getenv("POLARIS_NAMESPACE", "telemetry")
    table_name = os.getenv("POLARIS_TABLE", "gps_processed")
    endpoint = os.environ["MINIO_ENDPOINT"]
    if not endpoint.startswith(("http://", "https://")):
        endpoint = f"http://{endpoint}"

    spark = (
        SparkSession.builder
        .appName("street-speed-prediction")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(
            f"spark.sql.catalog.{catalog_name}",
            "org.apache.iceberg.spark.SparkCatalog",
        )
        .config(f"spark.sql.catalog.{catalog_name}.type", "rest")
        .config(f"spark.sql.catalog.{catalog_name}.uri", os.environ["POLARIS_URI"])
        .config(
            f"spark.sql.catalog.{catalog_name}.credential",
            os.environ["POLARIS_CREDENTIAL"],
        )
        .config(f"spark.sql.catalog.{catalog_name}.warehouse", catalog_name)
        .config(f"spark.sql.catalog.{catalog_name}.scope", "PRINCIPAL_ROLE:ALL")
        .config(
            f"spark.sql.catalog.{catalog_name}.io-impl",
            "org.apache.iceberg.aws.s3.S3FileIO",
        )
        .config(f"spark.sql.catalog.{catalog_name}.s3.endpoint", endpoint)
        .config(f"spark.sql.catalog.{catalog_name}.s3.path-style-access", "true")
        .config(
            f"spark.sql.catalog.{catalog_name}.s3.region",
            os.getenv("AWS_REGION", "us-east-1"),
        )
        .config(
            f"spark.sql.catalog.{catalog_name}.s3.access-key-id",
            os.environ["MINIO_ACCESS_KEY"],
        )
        .config(
            f"spark.sql.catalog.{catalog_name}.s3.secret-access-key",
            os.environ["MINIO_SECRET_KEY"],
        )
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        hour_buckets = []
        hour_cursor = cutoff.start_of("hour")
        while hour_cursor <= now:
            hour_buckets.append(hour_cursor)
            hour_cursor += timedelta(hours=1)

        table = spark.table(f"{catalog_name}.{namespace}.{table_name}")
        averages_df = (
            table.where(
                F.col("date").isin(
                    sorted({hour.format("YYYY-MM-DD") for hour in hour_buckets})
                )
                & F.col("hour").isin(sorted({hour.hour for hour in hour_buckets}))
                & (F.col("timestamp") >= cutoff_text)
                & (F.col("timestamp") <= now_text)
                & F.col("is_valid")
                & F.col("street_id").isNotNull()
                & F.col("speed_kmh").isNotNull()
            )
            .groupBy("street_id")
            .agg(F.avg("speed_kmh").alias("avg_speed"))
        )
        averages = [
            (int(row.street_id), float(row.avg_speed))
            for row in averages_df.collect()
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
                for offset in range(0, len(averages), 500):
                    batch = averages[offset:offset + 500]
                    values_sql = ", ".join(["(%s, %s)"] * len(batch))
                    parameters = [value for average in batch for value in average]
                    cursor.execute(
                        "UPDATE roads AS r "
                        "SET predicted_speed_kmh = v.avg_speed "
                        f"FROM (VALUES {values_sql}) AS v(osm_id, avg_speed) "
                        "WHERE r.osm_id = v.osm_id",
                        parameters,
                    )

        print(f"Updated rolling speed averages for {len(averages)} streets")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()