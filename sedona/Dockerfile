FROM docker.arvancloud.ir/apache/sedona:1.9.1

# ARG MAVEN_BASE=https://repo1.maven.org/maven2
ARG MAVEN_BASE=https://mirror-maven.runflare.com/maven2

ARG KAFKA_VERSION=3.9.1
ARG POSTGRES_JDBC_VERSION=42.7.7
ARG HADOOP_AWS_VERSION=3.4.1
ARG AWS_SDK_BUNDLE_VERSION=2.24.6
ARG ICEBERG_VERSION=1.10.2

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

WORKDIR /opt/workspace
COPY start-jobs.sh streaming_job.py lakehouse_job.py /opt/workspace/

ENTRYPOINT ["/usr/bin/bash", "/opt/workspace/start-jobs.sh"]
