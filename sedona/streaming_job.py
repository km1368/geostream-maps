"""Spark Structured Streaming job: validate + enrich GPS pings from Redpanda.

Reads the raw GPS stream from the `gps-raw` topic, flags pings whose GPS
accuracy error is too large (radius_error_m > MAX_RADIUS_ERROR_M) as invalid,
looks up the street each ping belongs to (nearest road to the ping's lat/lng,
spatial-joined against the PostGIS `roads` table) and adds it as `street_id`,
then writes the enriched events back to Redpanda on the `gps-processed` topic.
Each enriched event also carries `date` ("yyyy-MM-dd") and `hour` (int 0-23)
keys extracted from the ping timestamp, ready for day/hour aggregations
downstream.

Every micro-batch (fired every TRIGGER_INTERVAL, 15 s by default) also
aggregates the batch's valid pings per street and bulk-updates the `roads`
traffic columns: the average speed over the batch, the traffic level derived
from the speed ratio against the free-flow reference speed
(FREE_FLOW_SPEED_KMH), and the time of the update.

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
    f"jdbc:postgresql://postgres-1:5432/{os.environ.get('POSTGRES_DB', 'osm')}",
)
JDBC_PROPS = {
    "user": os.environ.get("POSTGRES_USER", "osm"),
    "password": os.environ.get("POSTGRES_PASSWORD", ""),
    "driver": "org.postgresql.Driver",
}
# Only look for streets within this many meters of a ping; pings farther away
# from every road get `street_id = null`.
MAX_STREET_DISTANCE_M = float(os.environ.get("MAX_STREET_DISTANCE_M", "100"))

# Free-flow reference speed for the traffic level: the ratio
# r = speed_kmh / FREE_FLOW_SPEED_KMH maps to 0 = free flow (r >= 0.8),
# 1 = slow (0.6 <= r < 0.8), 2 = heavy (0.4 <= r < 0.6), 3 = jammed (r < 0.4).
FREE_FLOW_SPEED_KMH = float(os.environ.get("FREE_FLOW_SPEED_KMH", "50"))

# How often the streaming query fires a micro-batch — and therefore how often
# the `roads` traffic columns are refreshed.
TRIGGER_INTERVAL = os.environ.get("TRIGGER_INTERVAL", "15 seconds")

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


def update_roads_traffic(stats_df):
    """Bulk-update the `roads` traffic columns from one batch's aggregates.

    The aggregated result is tiny (at most one row per street touched in the
    batch), so it is collected to the driver and applied as a single
    UPDATE ... FROM (VALUES ...) statement — one transaction per batch instead
    of one round trip per ping. Runs on the driver over JDBC; the Postgres
    driver is already registered with DriverManager by the startup JDBC read
    above (same driver jar shipped via --jars in docker-compose.yml).
    """
    rows = [
        row for row in stats_df.collect()
        if row["speed_kmh"] is not None and row["traffic_updated_at"] is not None
    ]
    if not rows:
        return

    def sql_literal(row):
        ts = str(row["traffic_updated_at"]).replace("'", "''")
        return (f"({int(row['street_id'])}, {row['speed_kmh']}, "
                f"{int(row['traffic_level'])}, '{ts}'::timestamptz)")

    sql = (
        "UPDATE roads AS r SET "
        "speed_kmh = v.speed_kmh, "
        "traffic_level = v.traffic_level, "
        "traffic_updated_at = v.traffic_updated_at "
        "FROM (VALUES " + ", ".join(sql_literal(row) for row in rows) + ") "
        "AS v(osm_id, speed_kmh, traffic_level, traffic_updated_at) "
        "WHERE r.osm_id = v.osm_id"
    )

    jvm = spark.sparkContext._jvm
    conn = jvm.java.sql.DriverManager.getConnection(
        JDBC_URL, JDBC_PROPS["user"], JDBC_PROPS["password"])
    try:
        stmt = conn.createStatement()
        try:
            updated = stmt.executeUpdate(sql)
        finally:
            stmt.close()
    finally:
        conn.close()
    print(f"Updated traffic for {updated} streets "
          f"({len(rows)} streets had valid pings in this batch)")


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
        # Calendar keys extracted from the ping timestamp: `date` as
        # "yyyy-MM-dd" and `hour` as an int 0-23 (null when the timestamp
        # does not parse). The generator sends naive local-time ISO
        # timestamps, so no timezone conversion is applied.
        .withColumn(
            "date", F.date_format(F.to_timestamp("timestamp"), "yyyy-MM-dd")
        )
        .withColumn("hour", F.hour(F.to_timestamp("timestamp")))
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
    # The result feeds both the Kafka write and the per-street aggregation
    # below, so cache it: the spatial join then runs only once per batch.
    enriched = (
        pings.join(nearest, "ping_id", "left")
        .withColumnRenamed("osm_id", "street_id")
        .withColumn(
            "is_valid",
            F.col("radius_error_m") <= F.lit(MAX_RADIUS_ERROR_M),
        )
        .persist()
    )

    try:
        enriched.select(
            F.to_json(
                F.struct(
                    F.col("timestamp"),
                    F.col("date"),
                    F.col("hour"),
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

        # Aggregate the batch's valid pings per street and refresh the roads
        # traffic columns: average speed over the batch, plus the traffic
        # level derived from the speed ratio against the free-flow speed.
        ratio = F.col("speed_kmh") / F.lit(FREE_FLOW_SPEED_KMH)
        street_stats = (
            enriched
            .filter(F.col("is_valid") & F.col("street_id").isNotNull())
            .groupBy("street_id")
            .agg(
                F.avg("speed_kmh").alias("speed_kmh"),
                F.max("timestamp").alias("traffic_updated_at"),
            )
            .withColumn(
                "traffic_level",
                F.when(ratio >= 0.8, F.lit(0))   # free flow
                .when(ratio >= 0.6, F.lit(1))    # slow
                .when(ratio >= 0.4, F.lit(2))    # heavy
                .otherwise(F.lit(3)),            # jammed
            )
        )
        update_roads_traffic(street_stats)
    finally:
        enriched.unpersist()


query = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", RAW_TOPIC)
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load()
    .writeStream
    .foreachBatch(enrich_batch)
    .trigger(processingTime=TRIGGER_INTERVAL)
    .option("checkpointLocation", CHECKPOINT)
    .start()
)

print(
    f"Streaming {RAW_TOPIC} -> {PROCESSED_TOPIC} every {TRIGGER_INTERVAL} "
    f"(is_valid = radius_error_m <= {MAX_RADIUS_ERROR_M}, "
    f"street_id = nearest road within {MAX_STREET_DISTANCE_M:.0f} m, "
    f"traffic_level = speed / {FREE_FLOW_SPEED_KMH:.0f} km/h ratio)"
)
query.awaitTermination()
