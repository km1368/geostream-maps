#!/bin/bash
# Executed by the official ClickHouse image entrypoint at startup.
# Every *.sh / *.sql file in /docker-entrypoint-initdb.d/ is run after the
# server is up -- but ONLY on first boot (empty data directory).
#
# Creates everything needed to consume from Redpanda:
#   kfk_gps_processed_aggregated - Kafka engine table (reads topic
#                                  'gps-processed' from redpanda-0:29092)
#   tbl_gps_processed_aggregated - AggregatingMergeTree (final storage)
#   mv_gps_processed_aggregated  - Materialized View moving data
#                                  kfk_gps_processed_aggregated -> tbl_...

echo "clickhouse-init: creating Kafka consumption pipeline..."

clickhouse-client --multiquery <<'SQL'
CREATE TABLE IF NOT EXISTS kfk_gps_processed_aggregated
(
    street_id   Int64,
    date        Date,
    hour        UInt8,
    speed_kmh   Decimal(5,2)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'redpanda-0:29092',
    kafka_topic_list = 'gps-processed',
    kafka_group_name = 'clickhouse-gps-processed',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1;

CREATE TABLE IF NOT EXISTS tbl_gps_processed_aggregated
(
    street_id  Int64,
    date       Date,
    hour       UInt8,
    speed_kmh  AggregateFunction(avg, Decimal(5,2))
)
ENGINE = AggregatingMergeTree
ORDER BY (street_id, date, hour);

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_gps_processed_aggregated
TO tbl_gps_processed_aggregated
AS SELECT
    street_id,
    date,
    hour,
    avgState(speed_kmh) AS speed_kmh
FROM kfk_gps_processed_aggregated
GROUP BY street_id, date, hour;
SQL

echo "clickhouse-init: done."
