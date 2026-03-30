# GHA Geodata — Open Geospatial Data Platform

ETL + API + CMS for the Greater Horn of Africa (11 IGAD+ countries).

## Architecture

```
gha-geodata/
├── etl/              # Standalone ETL package (no Django dependency)
│   ├── core/         # config, loaders, utils, metrics
│   ├── pipelines/    # gadm, jrc, dem, buildings, population, osm, hydrosheds, gridded
│   ├── orchestrator  # Phased parallel runner
│   └── cli           # Click CLI: python -m etl.cli extract dem
├── api/              # Django + Wagtail serving layer
│   ├── config/       # Django settings, urls
│   └── geodata/      # Models, REST API, Wagtail admin, mapviewer
├── docker/           # nginx, prometheus, grafana configs
└── notebooks/        # JupyterLab notebooks
```

## Two Independent Packages

### etl/ — `geodata-etl` (reusable in any system)
- `python -m etl.cli extract gadm` — download GADM boundaries
- `python -m etl.cli extract all` — run all extractors
- `python -m etl.orchestrator --only dem,population` — phased parallel ETL
- Reads config from env vars only. No Django dependency.
- Has its own `pyproject.toml`.

### api/ — Django + Wagtail
- REST API at `/api/geodata/`
- CMS admin at `/cms_admin/`
- Mapviewer at `/map/gha/{level}/`
- Bridge to ETL: `python manage.py run_etl --only gadm`

## Database

Single pgSTAC instance (PostGIS + STAC):
- Port 5433 (server) / configurable via `DB_PORT` env var
- Database: `geodata`, User: `geodata`
- Schemas: `gha` (cleaned), `africa` (continent), `geodata_raw` (GADM raw)

## Deployment

Server: `149.102.153.66` (SSH: `personal-playground`)
- API: http://149.102.153.66:8000
- CMS: http://149.102.153.66:8000/cms_admin/
- JupyterLab: http://149.102.153.66:8888/jupyter/lab
- Monitoring: http://149.102.153.66:8090 (Grafana)

### Docker Compose Profiles
```bash
docker compose up -d                              # db + api only
docker compose --profile etl run etl              # run ETL
docker compose --profile monitoring up -d         # add Prometheus/Grafana
```

### CI/CD
Push to `dev` → PR to `main` → auto-merge after checks → deploy

## Data Processing Rules
- All vectors clipped to GHA baseline (`gha.baseline`)
- All rasters masked to GHA baseline via rasterio
- Topology: ST_MakeValid → ST_SnapToGrid(1e-7) → ST_Buffer(0) → ST_MakeValid
- Tanzania = mainland + Zanzibar dissolved

## Preferences
- `uv` not `pip`
- Push to `dev`, PR to `main`
- No Co-Authored-By in commits
- Every notebook section: download → clip → push → plot
