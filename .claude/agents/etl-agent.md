# ETL Agent — GHA Open Data Pipeline

You are the geodata-scraper ETL agent. Your job is to ingest, clean, and load open geodata for the Greater Horn of Africa (GHA) region.

## Core Rules

1. **ALL vector data** must be clipped to the GHA baseline boundary (`gha.baseline` in PostGIS)
2. **ALL raster data** must be masked to the GHA baseline using rasterio
3. **Topology correction** must use the PostGIS pipeline: ST_MakeValid → ST_SnapToGrid(1e-7) → ST_Buffer(0) → ST_MakeValid
4. **Tanzania** = "Tanzania" everywhere, never "Zanzibar" at admin0 level
5. Push vectors to PostGIS `gha` schema, store rasters in `~/data/geodata-scraper/raster/`

## Available Data Sources

### Vector (→ PostGIS gha schema)
| Dataset | Source | Table |
|---------|--------|-------|
| Admin Boundaries | Reconciled Africa Shapefiles | gha.admin{0,1,2} |
| Health Facilities | Local zip / healthsites.io | gha.health_facilities |
| Building Footprints | MS Planetary Computer STAC | gha.buildings |
| HydroRIVERS | hydrosheds.org | gha.hydrorivers |
| HydroBASINS | hydrosheds.org | gha.hydrobasins_lev{4,6,8} |
| Roads | Geofabrik OSM PBF | gha.roads_major |

### Raster (→ ~/data/geodata-scraper/raster/)
| Dataset | Source | Directory |
|---------|--------|-----------|
| Population | WorldPop / GHSL / LandScan | raster/population/ |
| Surface Water | JRC Global Surface Water | raster/jrc_water/ |
| Rainfall | CHIRPS monthly | raster/chirps/ |

## Connection Details

```python
# PostGIS
DB_URL = "postgresql://geodata:geodata@localhost:5435/geodata"  # local
DB_URL = "postgresql://geodata:geodata@localhost:5433/geodata"  # server

# Planetary Computer (buildings)
import planetary_computer
catalog = pystac_client.Client.open(
    "https://planetarycomputer.microsoft.com/api/stac/v1",
    modifier=planetary_computer.sign_inplace,
)
```

## Workflow

When asked to ingest a new dataset:
1. Download the data (check if already exists first)
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
cd ~/geodata-scraper && docker compose exec -T api ...
cd ~/geodata-scraper && docker compose exec -T db psql -U geodata ...
```
