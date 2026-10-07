

# %% import libraries

import os
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from sedona.spark import SedonaContext

# %% create connections

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


def update_roads_traffic(stats_df):

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

    # convert to json
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
            .option("kafka.bootstrap.servers", "redpanda-1:29092") \
            .option("topic", "gps-processed") \
            .save()

        # groupBy street_id
        ratio = F.col("speed_kmh") / 60   # free flow speed = 60 km/h
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
    .option("kafka.bootstrap.servers", "redpanda-1:29092")
    .option("subscribe", "gps-raw")
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
