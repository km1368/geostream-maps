
echo "clickhouse-init: creating Kafka consumption pipeline..."

# NOTE: `timestamp` must be DateTime64, NOT DateTime. Spark writes ISO-8601
# timestamps with microsecond precision ("2026-10-02T21:39:09.489389") and
# DateTime cannot parse fractional seconds: the Kafka consumer then fails on
# EVERY message ("Cannot parse input: expected '"' ... while reading the
# value of key timestamp"), gets stuck retrying the same offset forever
# (system.kafka_consumers: num_messages_read grows, num_commits stays 0) and
# Redpanda console shows "There were issues deserializing the value".
clickhouse-client --multiquery <<'SQL'
CREATE TABLE IF NOT EXISTS kfk_gps_processed_aggregated
(
    street_id   Int64,
    timestamp   DateTime64,
    speed_kmh   Decimal(5,2)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'redpanda-1:29092',
    kafka_topic_list = 'gps-processed',
    kafka_group_name = 'clickhouse-gps-processed',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1;

CREATE TABLE IF NOT EXISTS tbl_gps_processed_aggregated
(
    street_id  Int64,
    timestamp  DateTime ,
    speed_kmh  Decimal(5,2)
)
ENGINE = MergeTree
ORDER BY (street_id, timestamp)
TTL timestamp + INTERVAL 6 HOUR;

-- streetless pings (street_id IS NULL) are dropped here so the non-Nullable
-- MergeTree table only holds rows that belong to a real street.
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_gps_processed_aggregated
TO tbl_gps_processed_aggregated
AS SELECT
    street_id,
    timestamp,
    speed_kmh
FROM kfk_gps_processed_aggregated
WHERE street_id IS NOT NULL;
SQL

echo "clickhouse-init: done."
