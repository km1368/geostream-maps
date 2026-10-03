local road_classes = {
    motorway = true,
    motorway_link = true,
    trunk = true,
    trunk_link = true,
    primary = true,
    primary_link = true,
    secondary = true,
    secondary_link = true
}

local roads = osm2pgsql.define_way_table('roads', {
    { column = 'osm_id',   type = 'int8',       not_null = true },
    { column = 'name',     type = 'text' },
    { column = 'name_fa',  type = 'text' },
    { column = 'name_en',  type = 'text' },
    { column = 'highway',  type = 'text',       not_null = true },
    { column = 'oneway',   type = 'text' },
    { column = 'maxspeed', type = 'text' },
    { column = 'lanes',    type = 'text' },
    { column = 'surface',  type = 'text' },
    {
        column = 'geom',
        type = 'linestring',
        projection = 4326,
        not_null = true
    }
})

-- ------------------------------------------------------------------
-- Water areas (lakes, reservoirs, rivers mapped as areas)
-- ------------------------------------------------------------------
local water_areas = osm2pgsql.define_area_table('water_areas', {
    { column = 'osm_id',  type = 'int8', not_null = true },
    { column = 'name',    type = 'text' },
    { column = 'water',   type = 'text' },
    {
        column = 'geom',
        type = 'multipolygon',
        projection = 4326,
        not_null = true
    }
})

-- ------------------------------------------------------------------
-- Green / natural areas (parks, forests, scrub, grass, jungle-like land)
-- ------------------------------------------------------------------
local green_areas = osm2pgsql.define_area_table('green_areas', {
    { column = 'osm_id',     type = 'int8', not_null = true },
    { column = 'name',       type = 'text' },
    { column = 'leisure',    type = 'text' },
    { column = 'landuse',    type = 'text' },
    { column = 'natural',    type = 'text' },
    {
        column = 'geom',
        type = 'multipolygon',
        projection = 4326,
        not_null = true
    }
})

local green_natural = {
    wood = true,
    forest = true,
    scrub = true,
    grassland = true,
    heath = true,
    wetland = true
}

local green_landuse = {
    forest = true,
    grass = true,
    meadow = true,
    farmland = true,
    orchard = true,
    vineyard = true
}

local green_leisure = {
    park = true,
    garden = true,
    pitch = true,
    golf_course = true,
    playground = true
}

local function is_closed_area(object)
    return object.is_closed and object.tags.area ~= 'no'
end

function osm2pgsql.process_relation(object)
    local tags = object.tags

    if tags.type ~= 'multipolygon' then
        return
    end

    if tags.natural == 'water' or tags.waterway == 'riverbank' or tags.landuse == 'reservoir' then
        water_areas:insert({
            osm_id = object.id,
            name = tags.name,
            water = tags.water,
            geom = object:as_multipolygon()
        })
        return
    end

    if green_natural[tags.natural] or green_landuse[tags.landuse] or green_leisure[tags.leisure] then
        green_areas:insert({
            osm_id = object.id,
            name = tags.name,
            leisure = tags.leisure,
            landuse = tags.landuse,
            natural = tags.natural,
            geom = object:as_multipolygon()
        })
    end
end

function osm2pgsql.process_way(object)
    local tags = object.tags

    -- Roads (ways only, no relations)
    local highway = tags.highway
    if highway and road_classes[highway] then
        roads:insert({
            osm_id = object.id,
            name = tags.name,
            name_fa = tags['name:fa'],
            name_en = tags['name:en'],
            highway = highway,
            oneway = tags.oneway,
            maxspeed = tags.maxspeed,
            lanes = tags.lanes,
            surface = tags.surface,
            geom = object:as_linestring()
        })
        return
    end

    -- Closed ways can also form water / green areas
    if not is_closed_area(object) then
        return
    end

    if tags.natural == 'water' or tags.waterway == 'riverbank' or tags.landuse == 'reservoir' then
        water_areas:insert({
            osm_id = object.id,
            name = tags.name,
            water = tags.water,
            geom = object:as_polygon()
        })
        return
    end

    if green_natural[tags.natural] or green_landuse[tags.landuse] or green_leisure[tags.leisure] then
        green_areas:insert({
            osm_id = object.id,
            name = tags.name,
            leisure = tags.leisure,
            landuse = tags.landuse,
            natural = tags.natural,
            geom = object:as_polygon()
        })
    end
end