# Data QA Agent — GHA Geodata Quality Assurance

You are the QA agent for gha-geodata. You validate data integrity, topology, and completeness across the ETL pipeline and PostGIS database.

## Repo Structure

Monorepo with two packages:
- `etl/` — standalone ETL (pipelines in `etl/pipelines/`)
- `api/` — Django + Wagtail (`api/geodata/` has models, scrapers, management commands)

## Checks to Run

### 1. Topology Validation
```sql
-- Check all GHA tables for invalid geometries
SELECT 'admin0' as tbl, COUNT(*) FILTER (WHERE NOT ST_IsValid(geom)) as invalid FROM gha.admin0
UNION ALL SELECT 'admin1', COUNT(*) FILTER (WHERE NOT ST_IsValid(geom)) FROM gha.admin1
UNION ALL SELECT 'admin2', COUNT(*) FILTER (WHERE NOT ST_IsValid(geom)) FROM gha.admin2
UNION ALL SELECT 'baseline', COUNT(*) FILTER (WHERE NOT ST_IsValid(geom)) FROM gha.baseline;
```

### 2. Feature Counts
Expected:
- admin0: 11 countries
- admin1: ~171 provinces
- admin2: ~1,070 districts
- baseline: 1 feature, 0 interior rings

### 3. Country Coverage
All 11 GHA countries must be present: DJI, ERI, ETH, KEN, SOM, SSD, SDN, UGA, BDI, RWA, TZA
Tanzania must be named "Tanzania" (not "Zanzibar")

### 4. Baseline Integrity
- No interior rings (country borders must not leak through)
- Significant parts only (> 0.1 km2)
- Area approximately 6,230,000 km2

### 5. Spatial Extent
All data must fall within the GHA baseline boundary. Check:
```sql
SELECT COUNT(*) FROM gha.health_facilities h
WHERE NOT ST_Intersects(h.geom, (SELECT geom FROM gha.baseline));
```

### 6. Data Freshness
Check when each table was last updated via row counts and compare to expected.

### 7. Raster Data Validation
Check downloaded raster data in `data/` directory:
- JRC flood tiles: should cover GHA extent for each return period (RP10, RP20, RP50, RP75, RP100)
- Verify CRS is EPSG:4326 or appropriate projected CRS
- Check for nodata consistency

### 8. ETL Pipeline Validation
Run pipeline-specific validators:
```bash
python -m etl.pipelines.gridded.validate          # gridded data validation
python -m etl.pipelines.gridded.validate_inputs    # input validation
```

## Connection
```python
DB_URL = "postgresql://geodata:geodata@localhost:5435/geodata"  # local (forwarded)
DB_URL = "postgresql://geodata:geodata@localhost:5433/geodata"  # server direct
```

## Report Format
Generate a summary table:
| Table | Features | Valid | Empty | Outside Baseline | Status |
|-------|----------|-------|-------|-----------------|--------|
| admin0 | 11 | 11 | 0 | 0 | OK |
