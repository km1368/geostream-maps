![GeoStream Maps logo](assets/logo.png)
## GeoStream Maps — A real-time data-driven platform for traffic monitoring

A simulator of map and real-time traffic services (similar to Google Maps) displaying live street traffic conditions on a map. Built as the final project of the data engineering course in Sepahram School of Data Engineering. 

### Data Engineering Stack

| Component   | Role  |
| ----------- | ------ |
| **Postgres + PostGIS** | Operational database. Stores geometry, current speed, current predicted speed, current color on the map for each street. |
| **Redpanda** | Real-time data broker. |
| **Spark + Sedona** | Maps GPS pings to streets, flags inaccurate ones, publishes enriched events to Redpanda, and hosts the standalone master/worker for Airflow jobs. |
| **ClickHouse** | Analytical database. Recent speed history per street (shown on click). Receives live data directly from Redpanda. |
| **Lakehouse (MinIO + Polaris + Iceberg)** | Storage for immutable events. Serves the job for forcasting the baseline speed for each street. |
| **Airflow** | Submits the five-minute street-speed prediction job to Sedona with `SparkSubmitOperator` every two minutes. |
| **Apache Superset** | Business Intelligence dashboards. |
| **Python** | Generates the live GPS stream (coordinates + speed of users); runs as the compose service `generator`. |

### Web interfaces

| Interface            | Local URL                     |
| --------------------- | ----------------------------- |
| Map                  | [http://localhost:8080](http://localhost:8080) |
| Redpanda Console     | [http://localhost:9300](http://localhost:9300) |
| Airflow              | [http://localhost:8081](http://localhost:8081) |
| Superset | [http://localhost:8088](http://localhost:8088) |
| Spark streaming UI | [http://localhost:4040](http://localhost:4040) |
| Spark lakehouse UI | [http://localhost:4041](http://localhost:4041) |
| Spark Connect UI | [http://localhost:4042](http://localhost:4042) |
| Spark master UI | [http://localhost:7079](http://localhost:7079) |
| Spark worker UI | [http://localhost:7080](http://localhost:7080) |

### Data flow

![dataflow](assets/dataflow.jpg)

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