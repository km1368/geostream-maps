FROM docker.arvancloud.ir/apache/superset:6.1.0

USER root
RUN pip --python /app/.venv/bin/python install --no-cache-dir -i https://mirror-pypi.runflare.com/simple \
    clickhouse-connect==0.9.2 psycopg2-binary==2.9.10
COPY superset_config.py /app/superset_config.py
COPY superset_init.py /app/superset_init.py
ENV SUPERSET_CONFIG_PATH=/app/superset_config.py
USER superset