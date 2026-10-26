#!/bin/sh
set -eu

RAW_DATA_DIR="/postgres/raw-map-data"
IRAN_PBF="$RAW_DATA_DIR/iran-latest.osm.pbf"
TEHRAN_PBF="$RAW_DATA_DIR/tehran.osm.pbf"
MAP_URL="${MAP_URL:-https://download.geofabrik.de/asia/iran-latest.osm.pbf}"
BBOX="${BBOX:-51.05,35.50,51.65,35.95}"

echo "Ensuring directory exists: $RAW_DATA_DIR"
mkdir -p "$RAW_DATA_DIR"

if [ ! -f "$IRAN_PBF" ] || [ "${FORCE_DOWNLOAD:-false}" = "true" ]; then
    echo "Downloading Iran map data from $MAP_URL..."
    wget -O "$IRAN_PBF.tmp" "$MAP_URL"
    mv "$IRAN_PBF.tmp" "$IRAN_PBF"
    echo "Downloaded $IRAN_PBF successfully."
else
    echo "File $IRAN_PBF already exists. Skipping download (set FORCE_DOWNLOAD=true to re-download)."
fi

# Check if trimmed Tehran map data already exists
if [ -f "$RAW_DATA_DIR/tehran.pbf" ]; then
    EXISTING_TEHRAN="$RAW_DATA_DIR/tehran.pbf"
elif [ -f "$TEHRAN_PBF" ]; then
    EXISTING_TEHRAN="$TEHRAN_PBF"
else
    EXISTING_TEHRAN=""
fi

if [ -z "$EXISTING_TEHRAN" ] || [ "${FORCE_TRIM:-false}" = "true" ]; then
    echo "Trimming Tehran map extract according to bounding box ($BBOX)..."
    osmium extract \
      --bbox "$BBOX" \
      --strategy complete_ways \
      "$IRAN_PBF" \
      --output "$TEHRAN_PBF" \
      --overwrite

    echo "Map preparation complete: $TEHRAN_PBF is ready."
else
    echo "File $EXISTING_TEHRAN already exists. Skipping trim (set FORCE_TRIM=true to re-trim)."
fi

