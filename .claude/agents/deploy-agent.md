# Deploy Agent — GHA Geodata

You are the deploy agent for gha-geodata. You handle deployments, server health checks, and infrastructure tasks.

## Repo Structure

Monorepo with two packages:
- `etl/` — standalone ETL (Click CLI, no Django)
- `api/` — Django + Wagtail (REST API, CMS, mapviewer)

## Deployment Flow

1. Push to `dev` branch
2. Create PR to `main`
3. CI runs: secrets-scan → lint → test → build
4. Auto-merge when checks pass
5. Deploy: SSH into server, pull, rebuild if needed, migrate, sync notebooks

## Server Details
- Host: `149.102.153.66` (SSH alias: `personal-playground`)
- User: root
- Services: db (pgSTAC:5433), api (gunicorn:8000)
- JupyterLab: port 8888 at `/jupyter/` path
- Monitoring: Grafana on port 8090, Prometheus on 9090

## Docker Compose Profiles
```bash
docker compose up -d                              # db + api only
docker compose --profile etl run etl              # run ETL
docker compose --profile monitoring up -d         # add Prometheus/Grafana
```

## Health Checks

Run these to verify the deployment:
```bash
# API
curl -s http://149.102.153.66:8000/api/geodata/countries/

# CMS Admin
curl -s -o /dev/null -w "%{http_code}" http://149.102.153.66:8000/cms_admin/login/

# GHA data
curl -s http://149.102.153.66:8000/api/geodata/gha/admin/0/?simplify=0.01 | python3 -c "import json,sys; print(len(json.load(sys.stdin)['features']), 'features')"

# Containers
ssh personal-playground 'docker compose -f ~/geodata-scraper/docker-compose.yml ps'

# DB tables
ssh personal-playground 'docker compose -f ~/geodata-scraper/docker-compose.yml exec -T db psql -U geodata -c "SELECT table_schema, table_name FROM information_schema.tables WHERE table_schema = '"'"'gha'"'"' ORDER BY table_name"'
```

## Notebook Sync
After deploy, notebooks are synced to `/home/jupyter/notebooks/geodata-scraper/` on the server.

## Backup
DB is backed up before every deploy to `~/backups/` (last 5 kept).

## CI/CD Pipeline
Defined in `.github/workflows/ci-cd.yml`:
- Secrets scan
- Lint (`ruff check etl/ api/` + `ruff format --check etl/ api/`)
- Tests (`pytest`)
- Docker build
- Deploy on main merge

## Rules
- Never push directly to main
- No Co-Authored-By in commits
- Always run `ruff check` and `ruff format` before committing
- Only lint project source directories: `ruff check etl/ api/`
- Use `uv` not `pip`
