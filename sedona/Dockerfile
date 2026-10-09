# Custom sedona-stream image: apache/sedona:1.9.1 + the connector/driver jars
# pre-fetched at BUILD time, so the container no longer depends on the
# bind-mounted ./sedona/jars folder on the host.
#
# Version constraints (do not bump casually):
#   - hadoop-aws MUST exactly match the hadoop-client-api version bundled in
#     the base image (3.4.1 for apache/sedona:1.9.1) — verify with:
#       docker run --rm <image> ls /opt/spark/jars | grep hadoop-client-api
#   - bundle-2.24.6 is the AWS SDK v2 bundle that hadoop-aws 3.4.1's POM
#     declares (software.amazon.awssdk:bundle).
#   - iceberg-spark-runtime must match Spark 4.0 / Scala 2.13;
#     iceberg-aws-bundle provides S3FileIO for the Polaris/MinIO catalog.
#
# Jars are pulled from Maven Central at build time. If the build host cannot
# reach repo1.maven.org, pre-seed the docker build cache or pass
# --build-arg MAVEN_BASE=https://<maven-mirror>/maven2.
FROM docker.arvancloud.ir/apache/sedona:1.9.1

ARG MAVEN_BASE=https://repo1.maven.org/maven2

ARG KAFKA_VERSION=3.9.1
ARG POSTGRES_JDBC_VERSION=42.7.7
ARG HADOOP_AWS_VERSION=3.4.1
ARG AWS_SDK_BUNDLE_VERSION=2.24.6
ARG ICEBERG_VERSION=1.10.2

# hadoop-client-api is already bundled in the base image — never download it.
# commons-pool2 ships in the image too, but at an older version; the Kafka
# connector wants 2.12.0, so we fetch our own copy.
ENV JARS_DIR=/opt/workspace/jars

RUN mkdir -p "$JARS_DIR" && \
    curl -fL --retry 5 --retry-delay 3 -o "$JARS_DIR/spark-sql-kafka-0-10_2.13-4.0.1.jar" \
      "$MAVEN_BASE/org/apache/spark/spark-sql-kafka-0-10_2.13/4.0.1/spark-sql-kafka-0-10_2.13-4.0.1.jar" && \
    curl -fL --retry 5 --retry-delay 3 -o "$JARS_DIR/spark-token-provider-kafka-0-10_2.13-4.0.1.jar" \
      "$MAVEN_BASE/org/apache/spark/spark-token-provider-kafka-0-10_2.13/4.0.1/spark-token-provider-kafka-0-10_2.13-4.0.1.jar" && \
    curl -fL --retry 5 --retry-delay 3 -o "$JARS_DIR/kafka-clients-${KAFKA_VERSION}.jar" \
      "$MAVEN_BASE/org/apache/kafka/kafka-clients/${KAFKA_VERSION}/kafka-clients-${KAFKA_VERSION}.jar" && \
    curl -fL --retry 5 --retry-delay 3 -o "$JARS_DIR/commons-pool2-2.12.0.jar" \
      "$MAVEN_BASE/org/apache/commons/commons-pool2/2.12.0/commons-pool2-2.12.0.jar" && \
    curl -fL --retry 5 --retry-delay 3 -o "$JARS_DIR/postgresql-${POSTGRES_JDBC_VERSION}.jar" \
      "$MAVEN_BASE/org/postgresql/postgresql/${POSTGRES_JDBC_VERSION}/postgresql-${POSTGRES_JDBC_VERSION}.jar" && \
    curl -fL --retry 5 --retry-delay 3 -o "$JARS_DIR/hadoop-aws-${HADOOP_AWS_VERSION}.jar" \
      "$MAVEN_BASE/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar" && \
    curl -fL --retry 5 --retry-delay 3 -o "$JARS_DIR/bundle-${AWS_SDK_BUNDLE_VERSION}.jar" \
      "$MAVEN_BASE/software/amazon/awssdk/bundle/${AWS_SDK_BUNDLE_VERSION}/bundle-${AWS_SDK_BUNDLE_VERSION}.jar" && \
    curl -fL --retry 5 --retry-delay 3 -o "$JARS_DIR/iceberg-spark-runtime-4.0_2.13-${ICEBERG_VERSION}.jar" \
      "$MAVEN_BASE/org/apache/iceberg/iceberg-spark-runtime-4.0_2.13/${ICEBERG_VERSION}/iceberg-spark-runtime-4.0_2.13-${ICEBERG_VERSION}.jar" && \
    curl -fL --retry 5 --retry-delay 3 -o "$JARS_DIR/iceberg-aws-bundle-${ICEBERG_VERSION}.jar" \
      "$MAVEN_BASE/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar" && \
    ls -la "$JARS_DIR"

# Bake the job scripts + launcher into the image as well, so sedona-stream no
# longer needs the ./sedona bind mounts. (Files are still mounted read-only in
# compose for the edit-and-restart workflow; the copies are a fallback.)
# NOTE: the compose entrypoint path stays /opt/workspace/start-jobs.sh either way.
WORKDIR /opt/workspace
COPY start-jobs.sh streaming_job.py lakehouse_job.py /opt/workspace/

# start-jobs.sh needs bash (wait -n); the base image ships bash 5.2 at
# /usr/bin/bash — same entrypoint as before, no compose change required.
ENTRYPOINT ["/usr/bin/bash", "/opt/workspace/start-jobs.sh"]
