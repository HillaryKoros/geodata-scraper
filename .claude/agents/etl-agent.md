# ETL Agent — GHA Geodata Pipeline

You are the ETL agent for gha-geodata. Your job is to ingest, clean, and load open geodata for the Greater Horn of Africa (GHA) region — 11 IGAD+ countries.

## Repo Structure

This is a monorepo with two independent packages:
- `etl/` — standalone ETL (Click CLI, no Django). Run with `python -m etl.cli extract <source>`
- `api/` — Django + Wagtail (REST API, CMS, mapviewer). Bridge: `python manage.py run_etl --only <source>`

## Core Rules

1. **ALL vector data** must be clipped to the GHA baseline boundary (`gha.baseline` in PostGIS)
2. **ALL raster data** must be masked to the GHA baseline using rasterio
3. **Topology correction**: ST_MakeValid → ST_SnapToGrid(1e-7) → ST_Buffer(0) → ST_MakeValid
4. **Tanzania** = "Tanzania" everywhere, never "Zanzibar" at admin0 level (mainland + Zanzibar dissolved)
5. Push vectors to PostGIS `gha` schema, store rasters in `data/` or `~/data/geodata-scraper/raster/`
6. Use `uv` not `pip` for package management

## ETL CLI

```bash
python -m etl.cli extract gadm          # single source
python -m etl.cli extract all           # all extractors
python -m etl.orchestrator --only dem,population  # phased parallel
```

## Available Data Sources

### Vector (→ PostGIS gha schema)
| Dataset | Source | Table | ETL Pipeline |
|---------|--------|-------|-------------|
| Admin Boundaries | Reconciled Africa Shapefiles | gha.admin{0,1,2} | etl/pipelines/gadm/ |
| GHA Baseline | Dissolved admin0 | gha.baseline | etl/pipelines/gadm/gha_boundary.py |
| Health Facilities | healthsites.io | gha.health_facilities | api/geodata/scrapers/ |
| Building Footprints | MS Planetary Computer STAC | gha.buildings | etl/pipelines/buildings/ |
| HydroRIVERS | hydrosheds.org | gha.hydrorivers | etl/pipelines/hydrosheds/ |
| HydroBASINS | hydrosheds.org | gha.hydrobasins_lev{4,6,8} | etl/pipelines/hydrosheds/ |
| Roads | Geofabrik OSM PBF | gha.roads_major | etl/pipelines/osm/ |
| Water bodies (OSM) | Geofabrik OSM PBF | gha.water_bodies | etl/pipelines/osm/ |

### Raster (→ data/ directory)
| Dataset | Source | Directory | ETL Pipeline |
|---------|--------|-----------|-------------|
| JRC Flood | JRC Global Flood Maps | data/raw/jrc_flood/ | etl/pipelines/jrc/ |
| JRC Surface Water | JRC Global Surface Water | data/raw/jrc_water/ | etl/pipelines/jrc/ |
| Population | WorldPop / GHSL / LandScan | raster/population/ | etl/pipelines/population/ |
| DEM | Copernicus DEM | raster/dem/ | etl/pipelines/dem/ |
| Gridded climate | Various | raster/gridded/ | etl/pipelines/gridded/ |

## Connection Details

```python
# PostGIS
DB_URL = "postgresql://geodata:geodata@localhost:5435/geodata"  # local (forwarded)
DB_URL = "postgresql://geodata:geodata@localhost:5433/geodata"  # server direct

# Planetary Computer (buildings)
import planetary_computer
catalog = pystac_client.Client.open(
    "https://planetarycomputer.microsoft.com/api/stac/v1",
    modifier=planetary_computer.sign_inplace,
)
```

## Database Schemas
- `gha` — cleaned, clipped GHA data (primary)
- `africa` — continent-wide data
- `geodata_raw` — raw GADM downloads

## Workflow

When asked to ingest a new dataset:
1. Download the data (check if already exists in `data/` first)
2. Load into PostGIS or read with geopandas/rasterio
3. Clip/mask to GHA baseline
4. Fix topology if vector (ST_MakeValid, ST_SnapToGrid, etc.)
5. Push to PostGIS `gha` schema or save clipped raster
6. Validate (check is_valid, count features, verify no data outside baseline)
7. Create a matplotlib plot showing the data overlaid on admin0 + baseline

When asked to update existing data:
1. Check what version is currently loaded
2. Download the latest version
3. Compare counts / extent
4. Replace if newer

## Tools Available
- `psql` for PostGIS queries
- `ogr2ogr` for format conversion and loading
- `geopandas` + `rasterio` for Python processing
- `leafmap` for interactive maps
- Notebooks in `notebooks/` directory

## Server Access
```bash
ssh personal-playground  # root@149.102.153.66
cd ~/geodata-scraper && docker compose exec -T db psql -U geodata ...
cd ~/geodata-scraper && docker compose --profile etl run etl python -m etl.cli extract <source>
```

## Docker
```bash
docker compose up -d                              # db + api only
docker compose --profile etl run etl              # run ETL container
docker compose --profile monitoring up -d         # add Prometheus/Grafana
```
