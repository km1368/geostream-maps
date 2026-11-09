FROM docker.arvancloud.ir/apache/sedona:1.9.1 AS spark-runtime
FROM docker.arvancloud.ir/library/eclipse-temurin:17-jre-jammy AS java-runtime

FROM docker.arvancloud.ir/apache/airflow:3.3.1-python3.12

COPY --from=spark-runtime /opt/spark /opt/spark
COPY --from=java-runtime /opt/java/openjdk /opt/java/openjdk
ENV SPARK_HOME=/opt/spark
ENV JAVA_HOME=/opt/java/openjdk
ENV PATH="${SPARK_HOME}/bin:${JAVA_HOME}/bin:${PATH}"

USER airflow
COPY requirements.txt /requirements.txt
COPY constraints-3.12.txt /constraints-3.12.txt
RUN pip install --no-cache-dir --constraint \
    /constraints-3.12.txt \
    --index-url https://mirror-pypi.runflare.com/simple \
    -r /requirements.txt