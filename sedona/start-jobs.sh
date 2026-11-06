#!/usr/bin/env bash
# Launch BOTH streaming jobs + a Spark Connect server in the sedona
# container.
#
# The main pipeline (streaming_job.py) and the lakehouse consumer
# (lakehouse_job.py) run as two independent spark-submits with their own
# checkpoints and Kafka consumer groups. Alongside them a Spark Connect
# server listens on :15002 so a laptop can attach a remote SparkSession
# (SparkSession.builder.remote) without running its own Spark image — it
# gets the same jars + Sedona/Iceberg/Polaris config as the jobs. If ANY of
# the three exits, the whole container exits so Docker's
# `restart: unless-stopped` relaunches everything — checkpoints make the
# restart cheap (no data loss, no reprocessing).
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

# Spark Connect server: remote SparkSessions from the host (localhost:15002).
# Same jar set as the lakehouse job (Kafka connector + Postgres JDBC + S3A +
# Iceberg) plus the Sedona + Iceberg SQL extensions and the polaris REST
# catalog, so remote sessions can do everything the jobs do. Launched as a
# plain foreground spark-submit — NOT sbin/start-connect-server.sh, which
# daemonizes and would escape `wait -n`. The bundled connect jar is the
# primary resource; a bare class name without --class is treated as a JAR
# path and fails with "Please specify one with --class".
echo "start-jobs: launching Spark Connect server (remote sessions, :15002)"
/opt/spark/bin/spark-submit \
    --master local[2] \
    --driver-memory 1g \
    --class org.apache.spark.sql.connect.service.SparkConnectServer \
    --jars "$S3A_JARS" \
    --conf spark.sql.extensions=org.apache.sedona.sql.SedonaSqlExtensions,org.apache.sedona.viz.sql.SedonaVizExtensions,org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
    --conf spark.sql.catalog.polaris=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.polaris.type=rest \
    --conf spark.sql.catalog.polaris.uri="${POLARIS_URI:-http://polaris-1:8181/api/catalog}" \
    --conf spark.sql.catalog.polaris.credential="${POLARIS_CREDENTIAL:-}" \
    --conf spark.sql.catalog.polaris.warehouse=polaris \
    --conf spark.sql.catalog.polaris.scope=PRINCIPAL_ROLE:ALL \
    --conf spark.sql.catalog.polaris.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
    --conf spark.sql.catalog.polaris.s3.endpoint="http://${MINIO_ENDPOINT:-minio-1:9000}" \
    --conf spark.sql.catalog.polaris.s3.path-style-access=true \
    --conf spark.sql.catalog.polaris.s3.region=us-east-1 \
    --conf spark.sql.catalog.polaris.s3.access-key-id="${MINIO_ACCESS_KEY:-}" \
    --conf spark.sql.catalog.polaris.s3.secret-access-key="${MINIO_SECRET_KEY:-}" \
    /opt/spark/jars/spark-connect_2.13-4.0.1.jar &
CONNECT_PID=$!

# Block until the first process exits, then tear the container down.
wait -n
STATUS=$?
echo "start-jobs: a job or the Connect server exited (status=$STATUS); stopping the rest"
kill "$MAIN_PID" "$LAKEHOUSE_PID" "$CONNECT_PID" 2>/dev/null
wait
exit "$STATUS"
