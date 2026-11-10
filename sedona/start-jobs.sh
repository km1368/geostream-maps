set -u

JARS_DIR=/opt/workspace/jars

export SPARK_MASTER_HOST=${SPARK_MASTER_HOST:-sedona-1}
export SPARK_LOCAL_HOSTNAME=${SPARK_LOCAL_HOSTNAME:-$SPARK_MASTER_HOST}
echo "start-jobs: launching standalone Spark master and worker"
/opt/spark/sbin/start-master.sh \
    --port 7077 \
    --webui-port 7079
/opt/spark/sbin/start-worker.sh \
    --port 7078 \
    --webui-port 7080 \
    --cores 1 \
    --memory 2G \
    "spark://$SPARK_MASTER_HOST:7077"

# Kafka connector + Postgres JDBC driver 
KAFKA_JARS="$JARS_DIR/spark-sql-kafka-0-10_2.13-4.0.1.jar,$JARS_DIR/spark-token-provider-kafka-0-10_2.13-4.0.1.jar,$JARS_DIR/kafka-clients-3.9.1.jar,$JARS_DIR/commons-pool2-2.12.0.jar,$JARS_DIR/postgresql-42.7.7.jar"
S3A_JARS="$KAFKA_JARS,$JARS_DIR/hadoop-aws-3.4.1.jar,$JARS_DIR/bundle-2.24.6.jar,$JARS_DIR/iceberg-spark-runtime-4.0_2.13-1.10.2.jar,$JARS_DIR/iceberg-aws-bundle-1.10.2.jar"

echo "start-jobs: launching main pipeline (streaming_job.py)"
/opt/spark/bin/spark-submit \
    --master local[2] \
    --conf spark.ui.port=4040 \
    --jars "$KAFKA_JARS" \
    /opt/workspace/streaming_job.py &
MAIN_PID=$!

echo "start-jobs: launching lakehouse consumer (lakehouse_job.py)"
/opt/spark/bin/spark-submit \
    --master local[2] \
    --conf spark.ui.port=4041 \
    --jars "$S3A_JARS" \
    /opt/workspace/lakehouse_job.py &
LAKEHOUSE_PID=$!

# Spark Connect server: remote SparkSessions from the host (localhost:15002).
echo "start-jobs: launching Spark Connect server (remote sessions, :15002)"
/opt/spark/bin/spark-submit \
    --master local[2] \
    --driver-memory 1g \
    --conf spark.ui.port=4042 \
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
