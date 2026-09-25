"""Spark Structured Streaming job: validate + enrich GPS pings from Redpanda.

Reads the raw GPS stream from the `gps-raw` topic, flags pings whose GPS
accuracy error is too large (radius_error_m > MAX_RADIUS_ERROR_M) as invalid,
looks up the street each ping belongs to (nearest road to the ping's lat/lng,
spatial-joined against the PostGIS `roads` table) and adds it as `street_id`,
then writes the enriched events back to Redpanda on the `gps-processed` topic.

Runs inside the `sedona-stream` container via spark-submit (see the
`sedona-stream` service in docker-compose.yml).
"""

import os

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from sedona.spark import SedonaContext

# Inside the compose network, Redpanda is reachable as `redpanda-0:29092`
# (internal listener). `localhost:9092` only works from the host.
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "redpanda-0:29092")
RAW_TOPIC = os.environ.get("RAW_TOPIC", "gps-raw")
PROCESSED_TOPIC = os.environ.get("PROCESSED_TOPIC", "gps-processed")
CHECKPOINT = os.environ.get("CHECKPOINT_LOCATION",
                            "/opt/workspace/.checkpoints/gps-validate")
# Pings with a GPS accuracy error above this many meters are considered invalid.
MAX_RADIUS_ERROR_M = float(os.environ.get("MAX_RADIUS_ERROR_M", "32"))

# PostGIS connection: street geometries live in the `roads` table (loaded by
# osm2pgsql). Credentials come from the container environment.
JDBC_URL = os.environ.get(
    "POSTGRES_JDBC_URL",
    f"jdbc:postgresql://db:5432/{os.environ.get('POSTGRES_DB', 'osm')}",
)
JDBC_PROPS = {
    "user": os.environ.get("POSTGRES_USER", "osm"),
    "password": os.environ.get("POSTGRES_PASSWORD", ""),
    "driver": "org.postgresql.Driver",
}
# Only look for streets within this many meters of a ping; pings farther away
# from every road get `street_id = null`.
MAX_STREET_DISTANCE_M = float(os.environ.get("MAX_STREET_DISTANCE_M", "100"))

spark = (
    SparkSession.builder
    .appName("gps-validate-stream")
    # The Kafka connector is passed via `--packages` on spark-submit (see the
    # `sedona-stream` service in docker-compose.yml); setting it here is not
    # reliable under spark-submit.
    .getOrCreate()
)
sedona = SedonaContext.create(spark)
spark.sparkContext.setLogLevel("WARN")

# ------------------------------------------------------------------
# Static street table from PostGIS: osm_id + geometry (fetched as WKB bytes,
# then parsed by Sedona). Tehran's ~16k roads are small enough to cache and
# spatial-join against every micro-batch.
# ------------------------------------------------------------------
roads = (
    spark.read.jdbc(
        JDBC_URL,
        "(SELECT osm_id, ST_AsBinary(geom) AS wkb FROM roads) AS roads_wkb",
        properties=JDBC_PROPS,
    )
    .withColumn("geom", F.expr("ST_GeomFromWKB(wkb)"))
    .select("osm_id", "geom")
    .persist()
)
road_count = roads.count()  # fail fast if the OSM import has not run yet
print(f"Loaded {road_count} streets from PostGIS for the street lookup")


def enrich_batch(batch_df, batch_df_id):
    """Validate pings and attach the nearest street id (spatial join)."""
    if batch_df.isEmpty():
        return

    # The Kafka source only exposes metadata columns (key, value, topic,
    # partition, offset, timestamp); the ping fields live as a JSON string in
    # `value`, so parse them out first.
    pings = (
        batch_df
        .withColumn(
            "ping",
            F.from_json(
                F.col("value").cast("string"),
                "timestamp STRING, latitude DOUBLE, longitude DOUBLE, "
                "speed_kmh DOUBLE, radius_error_m DOUBLE",
            ),
        )
        .select("ping.*")
        # One stable id per ping so the "nearest street" window can partition.
        .withColumn("ping_id", F.monotonically_increasing_id())
        .withColumn("geom", F.expr("ST_Point(longitude, latitude)"))
    )

    # Distance join: Sedona builds a spatial index on the static `roads` side.
    # ST_Distance on lon/lat geometry returns degrees, so the join predicate
    # uses a degree bound (meters converted at Tehran's latitude, where one
    # degree of longitude is ~90 km), while the ranking below uses the
    # spheroidal distance in meters.
    #
    # Built with the DataFrame API + F.expr rather than sedona.sql on temp
    # views: inside foreachBatch the batch DataFrame belongs to a per-batch
    # cloned SparkSession, so temp views registered here are not visible to
    # the outer session's sql() call.
    candidates = (
        pings.alias("p")
        .join(
            roads.alias("r"),
            F.expr(
                "ST_Distance(p.geom, r.geom) < "
                f"{MAX_STREET_DISTANCE_M / 90_000.0}"
            ),
        )
        .select(
            F.col("p.ping_id").alias("ping_id"),
            F.col("r.osm_id").alias("osm_id"),
            F.expr("ST_DistanceSpheroid(p.geom, r.geom)").alias("dist_m"),
        )
    )

    # Keep only the closest street per ping.
    nearest = (
        candidates.withColumn(
            "rank",
            F.row_number().over(
                Window.partitionBy("ping_id").orderBy("dist_m")
            ),
        )
        .filter("rank = 1")
        .select("ping_id", "osm_id")
    )

    # Flag pings with a GPS accuracy error above the threshold as invalid and
    # attach the looked-up street id (null when no street is within range).
    enriched = (
        pings.join(nearest, "ping_id", "left")
        .withColumnRenamed("osm_id", "street_id")
        .withColumn(
            "is_valid",
            F.col("radius_error_m") <= F.lit(MAX_RADIUS_ERROR_M),
        )
    )

    enriched.select(
        F.to_json(
            F.struct(
                F.col("timestamp"),
                F.col("latitude"),
                F.col("longitude"),
                F.col("speed_kmh"),
                F.col("radius_error_m"),
                F.col("is_valid"),
                F.col("street_id"),
            )
        ).alias("value")
    ).write.format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
        .option("topic", PROCESSED_TOPIC) \
        .save()


query = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", RAW_TOPIC)
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load()
    .writeStream
    .foreachBatch(enrich_batch)
    .option("checkpointLocation", CHECKPOINT)
    .start()
)

print(
    f"Streaming {RAW_TOPIC} -> {PROCESSED_TOPIC} "
    f"(is_valid = radius_error_m <= {MAX_RADIUS_ERROR_M}, "
    f"street_id = nearest road within {MAX_STREET_DISTANCE_M:.0f} m)"
)
query.awaitTermination()
