FROM docker.arvancloud.ir/apache/superset:6.1.0

USER root
ENV TZ=Asia/Tehran
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone
RUN . /app/.venv/bin/activate && \
    uv pip install --no-cache-dir --index-url https://mirror-pypi.runflare.com/simple \
    clickhouse-connect 
COPY superset_config.py /app/superset_config.py
COPY superset_init.py /app/superset_init.py
COPY dashboard-assets /app/dashboard-assets
USER superset
EXPOSE 8088
CMD ["/app/docker/entrypoints/run-server.sh"]
