
set -e

echo "clickhouse-init: creating Kafka consumption pipeline..."

# NOTE: `timestamp` must be DateTime64, NOT DateTime. Spark writes ISO-8601
# timestamps with microsecond precision ("2026-10-02T21:39:09.489389") and
# DateTime cannot parse fractional seconds: the Kafka consumer then fails on
# EVERY message ("Cannot parse input: expected '"' ... while reading the
# value of key timestamp"), gets stuck retrying the same offset forever
# (system.kafka_consumers: num_messages_read grows, num_commits stays 0) and
# Redpanda console shows "There were issues deserializing the value".
# Kafka engine tables cannot be altered, so recreate the consumer and view
# with the expanded event schema; the consumer group retains committed offsets.
clickhouse-client --multiquery <<'SQL'
DROP TABLE IF EXISTS mv_gps_processed_aggregated;
DROP TABLE IF EXISTS kfk_gps_processed_aggregated;

CREATE TABLE IF NOT EXISTS kfk_gps_processed_aggregated
(
    street_id   Int64,
    timestamp   DateTime64,
    speed_kmh   Decimal(5,2),
    is_valid    Bool
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
    speed_kmh  Decimal(5,2),
    is_valid   Nullable(Bool)
)
ENGINE = MergeTree
ORDER BY (street_id, timestamp)
TTL timestamp + INTERVAL 60 MINUTE;

ALTER TABLE tbl_gps_processed_aggregated
    ADD COLUMN IF NOT EXISTS is_valid Nullable(Bool);
ALTER TABLE tbl_gps_processed_aggregated
    MODIFY COLUMN is_valid Nullable(Bool);

-- streetless pings (street_id IS NULL) are dropped here so the non-Nullable
-- MergeTree table only holds rows that belong to a real street.
CREATE MATERIALIZED VIEW mv_gps_processed_aggregated
TO tbl_gps_processed_aggregated
AS SELECT
    street_id,
    timestamp,
    speed_kmh,
    is_valid
FROM kfk_gps_processed_aggregated
WHERE street_id IS NOT NULL;
SQL

echo "clickhouse-init: done."
