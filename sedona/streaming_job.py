# %% import libraries

import os
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from sedona.spark import SedonaContext

# %% create connections & configurations

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "redpanda-1:29092")
RAW_TOPIC = os.environ.get("RAW_TOPIC", "gps-raw")
PROCESSED_TOPIC = os.environ.get("PROCESSED_TOPIC", "gps-processed")

JDBC_URL = os.environ.get(
    "POSTGRES_JDBC_URL",
    f"jdbc:postgresql://postgres-1:5432/{os.environ.get('POSTGRES_DB', 'osm')}",
)
JDBC_PROPS = {
    "user": os.environ.get("POSTGRES_USER"),
    "password": os.environ.get("POSTGRES_PASSWORD"),
    "driver": "org.postgresql.Driver",
}

# %% create spark and sedona sessions

spark = (
    SparkSession.builder
    .appName("gps-validate-stream")
    .getOrCreate()
)
sedona = SedonaContext.create(spark)
spark.sparkContext.setLogLevel("WARN")

# %% load Tehran roads into memory

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

# %% update database

FREE_FLOW_SPEED_KMH = 60.0  # fallback baseline when no prediction exists yet


def fetch_predicted_speeds(street_ids):
    """osm_id -> predicted_speed_kmh for the streets the Airflow prediction
    DAG (street_speed_prediction) has written to roads.predicted_speed_kmh."""

    id_list = ", ".join(str(street_id) for street_id in street_ids)
    sql = (
        "SELECT osm_id, predicted_speed_kmh FROM roads "
        f"WHERE osm_id IN ({id_list}) AND predicted_speed_kmh IS NOT NULL"
    )

    jvm = spark.sparkContext._jvm
    conn = jvm.java.sql.DriverManager.getConnection(
        JDBC_URL, JDBC_PROPS["user"], JDBC_PROPS["password"])
    try:
        stmt = conn.createStatement()
        try:
            rs = stmt.executeQuery(sql)
            predicted = {}
            while rs.next():
                predicted[rs.getLong("osm_id")] = float(
                    rs.getFloat("predicted_speed_kmh"))
        finally:
            stmt.close()
    finally:
        conn.close()
    return predicted


def traffic_level_for(speed_kmh, predicted_kmh):
    """Traffic level from the measured speed relative to the street's
    predicted (normal) speed; streets without a prediction yet fall back to
    the fixed free-flow baseline."""

    baseline = (
        predicted_kmh
        if predicted_kmh is not None and predicted_kmh > 0
        else FREE_FLOW_SPEED_KMH
    )
    ratio = speed_kmh / baseline
    if ratio >= 0.8:
        return 0    # free flow
    if ratio >= 0.6:
        return 1    # slow
    if ratio >= 0.4:
        return 2    # heavy
    return 3        # jammed


def update_roads_traffic(stats_df):

    rows = [
        row.asDict() for row in stats_df.collect()
        if row["speed_kmh"] is not None and row["traffic_updated_at"] is not None
    ]
    if not rows:
        return

    # traffic_level = current speed vs the street's predicted speed
    predicted = fetch_predicted_speeds({int(row["street_id"]) for row in rows})
    for row in rows:
        row["traffic_level"] = traffic_level_for(
            row["speed_kmh"], predicted.get(int(row["street_id"])))

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

# %% lookup street from lat/lng

def enrich_batch(batch_df, batch_df_id):

    if batch_df.isEmpty():
        return

    # get pings in current batch
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
        .withColumn("ping_id", F.monotonically_increasing_id())
        .withColumn("geom", F.expr("ST_Point(longitude, latitude)"))
        .withColumn(
            "date", F.date_format(F.to_timestamp("timestamp"), "yyyy-MM-dd")
        )
        .withColumn("hour", F.hour(F.to_timestamp("timestamp")))
    )

    # find the streets closer than 100 meters for each ping
    candidates = (
        pings.alias("p")
        .join(
            roads.alias("r"),
            F.expr(
                "ST_Distance(p.geom, r.geom) < "
                f"{100 / 90_000.0}"
            ),
        )
        .select(
            F.col("p.ping_id").alias("ping_id"),
            F.col("r.osm_id").alias("osm_id"),
            F.expr("ST_DistanceSpheroid(p.geom, r.geom)").alias("dist_m"),
        )
    )

    # keep only the closest street per ping
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

    # flag pings with a GPS accuracy error above the threshold as invalid
    enriched = (
        pings.join(nearest, "ping_id", "left")
        .withColumnRenamed("osm_id", "street_id")
        .withColumn(
            "is_valid",
            # 30 = maximum allowed radius error
            F.col("radius_error_m") <= 30,
        )
        .persist()
    )

    try:
        # Publish enriched pings for downstream consumers, including lakehouse_job.py.
        processed = enriched.select(
            F.to_json(
                F.struct(
                    "timestamp",
                    "latitude",
                    "longitude",
                    "speed_kmh",
                    "radius_error_m",
                    "date",
                    "hour",
                    "is_valid",
                    "street_id",
                )
            ).alias("value")
        )
        (
            processed.write
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
            .option("topic", PROCESSED_TOPIC)
            .save()
        )

        # groupBy street_id — traffic_level is derived in update_roads_traffic
        # by comparing the measured speed against the street's predicted speed
        # (roads.predicted_speed_kmh, written by the Airflow DAG
        # street_speed_prediction).
        street_stats = (
            enriched
            .filter(F.col("is_valid") & F.col("street_id").isNotNull())
            .groupBy("street_id")
            .agg(
                F.avg("speed_kmh").alias("speed_kmh"),
                F.max("timestamp").alias("traffic_updated_at"),
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
    .trigger(processingTime="15 seconds")
    .option("checkpointLocation", "/opt/workspace/.checkpoints/gps-validate")
    .start()
)

query.awaitTermination()