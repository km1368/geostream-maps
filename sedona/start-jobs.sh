#!/usr/bin/env bash
# Launch BOTH streaming jobs in the sedona-stream container.
#
# The main pipeline (streaming_job.py) and the lakehouse consumer
# (lakehouse_job.py) run as two independent spark-submits with their own
# checkpoints and Kafka consumer groups. If EITHER job exits, the whole
# container exits so Docker's `restart: unless-stopped` relaunches both —
# checkpoints make the restart cheap (no data loss, no reprocessing).
#
# NOTE: needs bash (wait -n); the image ships bash 5.2 at /usr/bin/bash.
set -u

JARS_DIR=/opt/workspace/jars

# Kafka connector + Postgres JDBC driver (pre-fetched; --packages hangs on
# this network). The lakehouse job additionally needs the S3A filesystem:
# hadoop-aws MUST match the hadoop-client-api version bundled in the image
# (3.4.1 for apache/sedona:1.9.1) + the AWS SDK v2 bundle it declares.
# Iceberg/Polaris: iceberg-spark-runtime provides SparkCatalog + the streaming
# sink (must match Spark 4.0 / Scala 2.13); iceberg-aws-bundle provides
# S3FileIO for the REST catalog's MinIO storage.
KAFKA_JARS="$JARS_DIR/spark-sql-kafka-0-10_2.13-4.0.1.jar,$JARS_DIR/spark-token-provider-kafka-0-10_2.13-4.0.1.jar,$JARS_DIR/kafka-clients-3.9.1.jar,$JARS_DIR/commons-pool2-2.12.0.jar,$JARS_DIR/postgresql-42.7.7.jar"
S3A_JARS="$KAFKA_JARS,$JARS_DIR/hadoop-aws-3.4.1.jar,$JARS_DIR/bundle-2.24.6.jar,$JARS_DIR/iceberg-spark-runtime-4.0_2.13-1.10.2.jar,$JARS_DIR/iceberg-aws-bundle-1.10.2.jar"

echo "start-jobs: launching main pipeline (streaming_job.py)"
/opt/spark/bin/spark-submit \
    --master local[2] \
    --jars "$KAFKA_JARS" \
    /opt/workspace/streaming_job.py &
MAIN_PID=$!

echo "start-jobs: launching lakehouse consumer (lakehouse_job.py)"
/opt/spark/bin/spark-submit \
    --master local[2] \
    --jars "$S3A_JARS" \
    /opt/workspace/lakehouse_job.py &
LAKEHOUSE_PID=$!

# Block until the first job exits, then tear the container down.
wait -n
STATUS=$?
echo "start-jobs: a streaming job exited (status=$STATUS); stopping the other"
kill "$MAIN_PID" "$LAKEHOUSE_PID" 2>/dev/null
wait
exit "$STATUS"
