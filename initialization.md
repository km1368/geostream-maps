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

Start the database and wait until it is healthy:

```bash
docker compose up -d db
```

Run the one-time import, then apply post-import SQL:

```bash
docker compose --profile import run --rm importer
```

Start the remaining services (Martin tile server + web UI):

```bash
docker compose up -d
```

### 4. Verify

Open the URL 

```bash
http://localhost:8080
```
