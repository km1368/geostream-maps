![GeoStream Maps logo](assets/logo.png)
## GeoStream Maps — A real-time data-driven platform for traffic monitoring

A simulator of map and real-time traffic services (similar to Google Maps) displaying live street traffic conditions on a map. Built as the final project of the data engineering course in Sepahram School of Data Engineering. 

### Data Engineering Stack

| Component   | Role  |
| ----------- | ------ |
| **Postgres + PostGIS** | Operational database. Stores geometry, current speed, current predicted speed, current color on the map for each street. |
| **Redpanda** | Real-time data broker. |
| **Spark + Sedona** | Stream processing: map GPS pings to streets, flag inaccurate ones, and publishes enriched events back to Redpanda. |
| **ClickHouse** | Analytical database. Recent speed history per street (shown on click). Receives live data directly from Redpanda. |
| **Lakehouse (MinIO + Polaris + Iceberg)** | Storage for immutable events. Serves the job for forcasting the baseline speed for each street. |
| **Airflow** | Orchestrates the job for forcasting the baseline speed for each street. |
| **Apache Superset** | Business Intelligence dashboards. |
| **Python** | Generates the live GPS stream (coordinates + speed of users); runs as the compose service `generator`. |

### Web interfaces

| Interface            | Local URL                     |
| --------------------- | ----------------------------- |
| Map                  | [http://localhost:8080](http://localhost:8080) |
| Redpanda Console     | [http://localhost:9300](http://localhost:9300) |
| Airflow              | [http://localhost:8081](http://localhost:8081) |
| Superset | [http://localhost:8088](http://localhost:8088) |

### Data flow

```mermaid
flowchart TD
    SIM["Python simulator<br/>GPS pings: position + speed"] --> RAW["Redpanda<br/>topic: gps-raw"]
    RAW --> SP["Spark + Sedona<br/>flag invalid GPS,<br/>spatial-join ping to street"]
    SP --> ENR["Redpanda<br/>topic: gps-processed"]
    ENR --> PG[("Postgres + PostGIS<br/>latest street status:<br/>current + baseline duration, color")]
    ENR --> CH["ClickHouse<br/>raw pings per street + TTL 6h<br/>speed trend per street"]
    ENR --> S3[("MinIO + Iceberg/Polaris<br/>lakehouse: gps_processed")]
    S3 --> PRED["Prediction job (every 2 min)<br/>5-minute average speed per street"]
    PRED --> PG
    AF["Airflow"] -.->|"orchestrates"| PRED
```

### UI Stack

- **Nginx** — serves the static page and proxies tile/history requests.
- **HTML/CSS + MapLibre GL JS** — renders the map and live traffic colors.
- **Martin** — tile server generating vector tiles straight from PostGIS.

```mermaid
flowchart TD
    U["Browser<br/>HTML/CSS + MapLibre GL JS"] -->|"HTTP requests"| NG["Nginx"]
    NG -->|"static page"| U
    NG -->|"/tiles/*"| MA["Martin tile server"]
    MA -->|"vector tiles"| PG[("Postgres + PostGIS<br/>street geometry + live status")]
    NG -->|"/clickhouse/ on street click"| CH["ClickHouse<br/>recent speeds per street"]
```

### Initialization

```bash
docker compose --profile import run --rm download-and-trim-map;
docker compose up -d postgres-1;
docker compose --profile import run --rm import-map-to-postgres;
docker compose --profile import run --rm --no-deps create-postgres-columns;
docker compose up -d;
docker compose wait minio-init polaris-init airflow-db-init;
docker compose rm -f minio-init polaris-init airflow-db-init;
```