ALTER TABLE roads
    ADD COLUMN IF NOT EXISTS traffic_level smallint,
    ADD COLUMN IF NOT EXISTS speed_kmh real,
    ADD COLUMN IF NOT EXISTS traffic_updated_at timestamptz;

CREATE UNIQUE INDEX IF NOT EXISTS roads_osm_id_idx
    ON roads (osm_id);

CREATE INDEX IF NOT EXISTS roads_geom_idx
    ON roads USING gist (geom);

CREATE INDEX IF NOT EXISTS roads_highway_idx
    ON roads (highway);

CREATE INDEX IF NOT EXISTS roads_traffic_level_idx
    ON roads (traffic_level);

CREATE INDEX IF NOT EXISTS water_areas_geom_idx
    ON water_areas USING gist (geom);

CREATE INDEX IF NOT EXISTS green_areas_geom_idx
    ON green_areas USING gist (geom);

ANALYZE roads;
ANALYZE water_areas;
ANALYZE green_areas;
