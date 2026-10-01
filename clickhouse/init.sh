#!/bin/bash
# Executed by the official ClickHouse image entrypoint at startup.
# Every *.sh / *.sql file in /docker-entrypoint-initdb.d/ is run after the
# server is up -- but ONLY on first boot (empty data directory).
#
# Creates everything needed to consume from Redpanda:
#   events_raw - Kafka engine table (reads topic 'events' from redpanda:9092)
#   events     - MergeTree table (final storage)
#   events_mv  - Materialized View moving data events_raw -> events

echo "clickhouse-init: creating Kafka consumption pipeline..."

clickhouse-client --multiquery <<'SQL'
CREATE TABLE IF NOT EXISTS events_raw
(
    event_id   String,
    user_id    String,
    event_type String,
    amount     Float64,
    ts         String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'redpanda:9092',
    kafka_topic_list = 'events',
    kafka_group_name = 'clickhouse-events',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1;

CREATE TABLE IF NOT EXISTS events
(
    event_id   String,
    user_id    LowCardinality(String),
    event_type LowCardinality(String),
    amount     Float64,
    ts         DateTime64(6, 'UTC')
)
ENGINE = MergeTree
ORDER BY (user_id, ts);

CREATE MATERIALIZED VIEW IF NOT EXISTS events_mv
TO events
AS SELECT
    event_id,
    user_id,
    event_type,
    amount,
    parseDateTime64BestEffort(ts) AS ts
FROM events_raw;
SQL

echo "clickhouse-init: done."
