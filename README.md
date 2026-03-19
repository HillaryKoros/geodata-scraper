# geodata-scraper

GIS ELT pipeline for PostGIS — Extract geodata from any source, Load into PostGIS, Transform in-database.

## Architecture

```
Source (HTTPS/API/FTP) → Extract → Load (ogr2ogr → PostGIS) → Transform (SQL) → Serve (API/Parquet)
```

4 independent Docker services:
- **db** — pgSTAC/PostGIS database
- **scraper** — ELT pipeline worker
- **api** — REST API (Django/DRF)
- **notebook** — Jupyter for exploration

## Quick Start

```bash
# Clone and start
git clone https://github.com/icpac-igad/geodata-scraper.git
cd geodata-scraper
cp .env.example .env
docker compose up -d

# The scraper auto-runs the full pipeline:
# 1. GADM admin boundaries (11 IGAD+ countries, all levels)
# 2. Create merged/dissolved admin0 baseline
# 3. HydroSHEDS basins (12 levels) + rivers, clipped to IGAD+
# 4. Export Parquet files
```

## Scrapers

| Scraper | Source | Data |
|---|---|---|
| `gadm` | GADM v4.1 | Admin boundaries (global, levels 0-5) |
| `hydrosheds` | HydroSHEDS | Basins (12 levels) + rivers |
| `http` | Any HTTPS URL | Generic geodata |
| `api` | REST API | GeoJSON with pagination |
| `ftp` | FTP/SFTP | Remote files |

## CLI

```bash
# Scrape GADM
docker compose exec scraper python manage.py scrape gadm --region igad_plus

# Scrape HydroSHEDS (all 12 basin levels + rivers, clipped to IGAD+)
docker compose exec scraper python manage.py scrape_hydrosheds

# Create baseline boundary (merged/dissolved/simplified admin0)
docker compose exec scraper python manage.py create_baseline

# Export to Parquet
docker compose exec scraper python manage.py export_parquet --region igad_plus --output /data/parquet

# List available scrapers
docker compose exec scraper python manage.py list_sources
```

## API Endpoints

```
GET /api/geodata/countries/                — list countries with data
GET /api/geodata/boundaries/{iso3}/        — admin levels for a country
GET /api/geodata/boundaries/{iso3}/{level}/ — GeoJSON (?simplify=, ?bbox=, ?limit=)
GET /api/geodata/layers/                   — all ingested layers
GET /api/geodata/layers/{id}/geojson/      — GeoJSON for any layer
GET /api/geodata/sources/                  — data sources
GET /api/geodata/jobs/                     — scrape job history
```

## Data Sources Registry

See `geodata_scraper/fixtures/sources.json` for the full catalog with URLs, layer schemas, property definitions, and storage patterns.

## Deploy to Server

```bash
# On your server
git clone https://github.com/icpac-igad/geodata-scraper.git
cd geodata-scraper
cp .env.example .env
# Edit .env with production values
docker compose up -d
```

## License

MIT
