FROM docker.arvancloud.ir/library/python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_INDEX_URL=https://mirror-pypi.runflare.com/simple \
    PIP_DEFAULT_TIMEOUT=60

WORKDIR /app

# Dependencies first: code edits rebuild from the COPY layer below.
RUN pip install --no-cache-dir numpy kafka-python python-dotenv

COPY generator.py hotspots.csv ./

ENV KAFKA_BOOTSTRAP=localhost:9092

CMD ["python", "generator.py"]
