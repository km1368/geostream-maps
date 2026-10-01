# Initialization

Imports OpenStreetMap data into PostGIS and serves it as vector tiles with Martin +  MapLibre UI.

### 1. Download raw data

Download a Iran's raw open streets map data into  `./data/` :

```bash
wget https://download.geofabrik.de/asia/iran-latest.osm.pbf
```

### 2. Extract Tehran's data

Extract only Tehran's data to keep the map lightweight. 

```bash
osmium extract \
  --bbox 51.05,35.50,51.65,35.95 \
  --strategy complete_ways \
  iran-latest.osm.pbf \
  --output data/tehran.osm.pbf
```

### 3. Start the services in sequence

Start the postgres database, then import streets and other data from osm data into tables, then add columns for the current status of streets, and finally, bring the cluster up:

```bash
docker compose up -d db; 
docker compose --profile import run --rm importer; 
docker compose --profile import run --rm --no-deps post_import; 
docker compose up -d;
```

### 4. Verify on the map

Open the URL 

```bash
http://localhost:8080
```

### 5. Verify in ClickHouse

```SQL
SELECT street_id, date, hour, avgMerge(speed_kmh) AS avg_speed_kmh
FROM default.tbl_gps_processed_aggregated
GROUP BY street_id, date, hour ;
```
