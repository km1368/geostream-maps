# %% lakehouse job: consume gps-processed into the MinIO lakehouse using Iceberg & Polaris
#
# Runs as a SECOND spark-submit inside the sedona container.
# Layout: Managed by Apache Iceberg via Apache Polaris (REST Catalog).
# Writes ALL events (invalid pings and streetless included).

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# %% config from environment (compose overrides these)

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "redpanda-1:29092")
SOURCE_TOPIC = os.environ.get("PROCESSED_TOPIC", "gps-processed")
CONSUMER_GROUP = os.environ.get("LAKEHOUSE_GROUP", "spark-lakehouse")
CHECKPOINT = os.environ.get("LAKEHOUSE_CHECKPOINT", "/opt/workspace/.checkpoints/gps-lakehouse-iceberg")
TRIGGER_INTERVAL = os.environ.get("LAKEHOUSE_TRIGGER", "60 seconds")

# MinIO Configs
BUCKET = os.environ.get("LAKEHOUSE_BUCKET", "lakehouse")
PREFIX = os.environ.get("LAKEHOUSE_PREFIX", "gps_processed")
ENDPOINT = os.environ.get("MINIO_ENDPOINT", "minio-1:9000")
ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "")
SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "")

# Polaris / Iceberg Configs
POLARIS_URI = os.environ.get("POLARIS_URI", "http://polaris-1:8181/api/catalog")
POLARIS_CREDENTIAL = os.environ.get("POLARIS_CREDENTIAL", "client_id:client_secret")
CATALOG_NAME = os.environ.get("ICEBERG_CATALOG", "polaris")
NAMESPACE = os.environ.get("ICEBERG_NAMESPACE", "telemetry")
TABLE_NAME = os.environ.get("ICEBERG_TABLE", "gps_processed")

FULL_TABLE_IDENTIFIER = f"{CATALOG_NAME}.{NAMESPACE}.{TABLE_NAME}"

# %% create spark session with S3A and Iceberg/Polaris wired up
#
# We configure Iceberg to use the REST catalog pointing to Polaris.
# S3FileIO (iceberg-aws-bundle jar) reads the fs.s3a.* configs below to talk
# to MinIO; Polaris vends the table location, we supply the credentials.

spark = (
    SparkSession.builder
    .appName("gps-lakehouse-stream-iceberg")
    # Iceberg SQL Extensions
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")

    # Polaris (REST Catalog) configurations
    .config(f"spark.sql.catalog.{CATALOG_NAME}", "org.apache.iceberg.spark.SparkCatalog")
    .config(f"spark.sql.catalog.{CATALOG_NAME}.type", "rest")
    .config(f"spark.sql.catalog.{CATALOG_NAME}.uri", POLARIS_URI)
    .config(f"spark.sql.catalog.{CATALOG_NAME}.credential", POLARIS_CREDENTIAL)
    .config(f"spark.sql.catalog.{CATALOG_NAME}.warehouse", CATALOG_NAME)
    .config(f"spark.sql.catalog.{CATALOG_NAME}.scope", "PRINCIPAL_ROLE:ALL")
    .config(f"spark.sql.catalog.{CATALOG_NAME}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    .config(f"spark.sql.catalog.{CATALOG_NAME}.s3.endpoint", f"http://{ENDPOINT}")
    .config(f"spark.sql.catalog.{CATALOG_NAME}.s3.path-style-access", "true")
    .config(f"spark.sql.catalog.{CATALOG_NAME}.s3.region", "us-east-1")
    .config(f"spark.sql.catalog.{CATALOG_NAME}.s3.access-key-id", ACCESS_KEY)
    .config(f"spark.sql.catalog.{CATALOG_NAME}.s3.secret-access-key", SECRET_KEY)
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# %% Initialize Table in Polaris
# Unlike raw Parquet where you partition on write, Iceberg tables define their 
# partition scheme centrally in the catalog. We initialize it here if it doesn't exist.

spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG_NAME}.{NAMESPACE}")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FULL_TABLE_IDENTIFIER} (
    timestamp STRING, 
    latitude DOUBLE, 
    longitude DOUBLE,
    speed_kmh DOUBLE, 
    radius_error_m DOUBLE,
    date STRING, 
    hour INT,
    is_valid BOOLEAN, 
    street_id BIGINT,
    kafka_timestamp TIMESTAMP
)
USING iceberg
PARTITIONED BY (date, hour)
""")

# %% schema — keep in sync with streaming_job.py

PAYLOAD_SCHEMA = """
    timestamp STRING, latitude DOUBLE, longitude DOUBLE,
    speed_kmh DOUBLE, radius_error_m DOUBLE,
    date STRING, hour INT,
    is_valid BOOLEAN, street_id BIGINT
"""

# %% consume

df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", SOURCE_TOPIC)
    .option("kafka.group.id", CONSUMER_GROUP)
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .load()
    .select(
        F.from_json(F.col("value").cast("string"), PAYLOAD_SCHEMA).alias("ping"),
        F.col("timestamp").alias("kafka_timestamp"),
    )
    .select("ping.*", "kafka_timestamp")
)

# Iceberg provides a native streaming sink. It automatically handles 
# append/commits correctly, replacing the need for foreachBatch.
query = (
    df.writeStream
    .format("iceberg")
    .outputMode("append")
    .trigger(processingTime=TRIGGER_INTERVAL)
    .option("checkpointLocation", CHECKPOINT)
    .toTable(FULL_TABLE_IDENTIFIER)
)

print(f"lakehouse job: streaming {SOURCE_TOPIC} from {KAFKA_BOOTSTRAP} to Iceberg table {FULL_TABLE_IDENTIFIER} "
      f"(group={CONSUMER_GROUP}, trigger={TRIGGER_INTERVAL})")

query.awaitTermination()