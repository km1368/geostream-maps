---
description: "Use when working on the GPS telemetry generator, Tehran simulation routes, hotspots, or Redpanda raw message publishing."
applyTo: "generator/**"
---

- The generator simulates Tehran GPS telemetry and publishes JSON messages to the Redpanda topic `gps-raw`.
- Keep the payload keys synchronized with the Kafka parsing schema in `sedona/streaming_job.py`.
- Support both Compose and host runs: use `KAFKA_BOOTSTRAP` with default `localhost:9092`; Compose sets `redpanda-1:29092`.
- Compose sets `TZ=Asia/Tehran`; preserve local timestamps because downstream date/hour partitioning depends on them.
- The generator image copies its source and hotspot data at build time; rebuild the `generator` service after editing either.
- Flush the Kafka producer on graceful shutdown and handle interruption without leaving buffered messages behind.