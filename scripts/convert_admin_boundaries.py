"""
Convert Sub-Saharan Africa admin boundary shapefiles to topology-correct
GeoParquet + GeoJSON for GHA region.

Pipeline:
  1. Load shapefiles with QGIS processing (v.clean for topology)
  2. Push raw to PostGIS (africa schema)
  3. Use PostGIS functions for topology correction:
     - ST_MakeValid, ST_SnapToGrid, ST_SimplifyPreserveTopology
     - ST_Buffer(0) to fix ring issues
  4. Filter GHA countries in PostGIS
  5. Export corrected data to parquet + geojson

Usage:
    python scripts/convert_admin_boundaries.py
    python scripts/convert_admin_boundaries.py --push
"""

import argparse
import subprocess
import sys
from pathlib import Path

import geopandas as gpd

SRC_DIR = Path("/home/koros/Data/Sub_Saharan_Admin")
OUT_DIR = Path("/home/koros/IGAD-ICPAC/Projects/geodata-scraper/data")

GHA_ISO3 = (
    "'DJI','ERI','ETH','KEN','SOM','SSD','SDN','UGA','BDI','RWA','TZA'"
)
GHA_ISO3_LIST = ["DJI", "ERI", "ETH", "KEN", "SOM", "SSD", "SDN", "UGA", "BDI", "RWA", "TZA"]

DB_LOCAL = "postgresql://geodata:geodata@localhost:5435/geodata"
DB_REMOTE = "postgresql://geodata:geodata@149.102.153.66:5433/geodata"

OGR_PG_LOCAL = "PG:host=localhost port=5435 dbname=geodata user=geodata password=geodata"

LEVELS = {
    0: {
        "file": "Africa_&_Islands_Admin0_boundaries_reconciled_(061223).shp",
        "iso_col": "gid_0",
    },
    1: {
        "file": "Africa_&_Islands_Admin1_boundaries_reconciled_(061223).shp",
        "iso_col": "gid_0",
    },
    2: {
        "file": "Africa_&_Islands_Admin2_boundaries_reconciled_(061223).shp",
        "iso_col": "gid_0",
    },
}

SNAP_GRID = 0.0000001  # ~1cm precision
SIMPLIFY_TOLERANCE = 0.001  # ~100m for geojson


def run_sql(sql, db_url=DB_LOCAL):
    """Run SQL via psql."""
    # Parse connection from URL
    from urllib.parse import urlparse
    p = urlparse(db_url)
    cmd = [
        "psql",
        "-h", p.hostname,
        "-p", str(p.port),
        "-U", p.username,
        "-d", p.path.lstrip("/"),
        "-c", sql,
    ]
    env = {"PGPASSWORD": p.password, "PATH": "/usr/bin:/usr/local/bin"}
    result = subprocess.run(cmd, capture_output=True, text=True, env=env)
    if result.returncode != 0:
        print(f"  SQL ERROR: {result.stderr.strip()}")
    return result


def step1_load_to_postgis():
    """Load raw shapefiles into PostGIS africa_raw schema via ogr2ogr."""
    print("=" * 60)
    print("  Step 1: Load shapefiles to PostGIS (africa_raw)")
    print("=" * 60)

    run_sql("CREATE SCHEMA IF NOT EXISTS africa_raw;")

    for level, config in LEVELS.items():
        src = SRC_DIR / config["file"]
        table = f"admin{level}"
        print(f"\n  Loading {src.name} -> africa_raw.{table}")

        cmd = [
            "ogr2ogr", "-f", "PostgreSQL", OGR_PG_LOCAL,
            str(src),
            "-nln", f"africa_raw.{table}",
            "-overwrite",
            "-nlt", "PROMOTE_TO_MULTI",
            "-lco", "GEOMETRY_NAME=geom",
            "-lco", "FID=gid",
            "-a_srs", "EPSG:4326",
            "-progress",
        ]
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            print(f"    ERROR: {result.stderr.strip()}")
        else:
            print(f"    OK")


def step2_fix_topology_postgis():
    """Use PostGIS functions for topology correction."""
    print("\n" + "=" * 60)
    print("  Step 2: Fix topology in PostGIS")
    print("=" * 60)

    run_sql("CREATE SCHEMA IF NOT EXISTS africa;")
    run_sql("CREATE SCHEMA IF NOT EXISTS gha;")

    for level in LEVELS:
        table = f"admin{level}"
        iso_col = LEVELS[level]["iso_col"]
        print(f"\n  Fixing africa_raw.{table}...")

        # Get all columns except geometry
        result = run_sql(f"""
            SELECT string_agg('"' || column_name || '"', ', ')
            FROM information_schema.columns
            WHERE table_schema = 'africa_raw'
              AND table_name = '{table}'
              AND column_name NOT IN ('geom', 'gid')
        """)

        # Create corrected africa table
        run_sql(f"""
            DROP TABLE IF EXISTS africa.{table} CASCADE;
            CREATE TABLE africa.{table} AS
            SELECT
                *,
                -- Topology correction pipeline:
                -- 1. MakeValid (fix self-intersections)
                -- 2. SnapToGrid (align vertices, prevent micro-gaps)
                -- 3. Buffer(0) (fix ring orientation issues)
                -- 4. MakeValid again (cleanup after snap)
                ST_Multi(
                    ST_MakeValid(
                        ST_Buffer(
                            ST_SnapToGrid(
                                ST_MakeValid(geom),
                                {SNAP_GRID}
                            ),
                            0
                        )
                    )
                ) AS geom_clean,
                ST_Area(ST_MakeValid(geom)::geography) / 1e6 AS area_km2
            FROM africa_raw.{table}
            WHERE geom IS NOT NULL;

            -- Replace original geom with cleaned version
            ALTER TABLE africa.{table} DROP COLUMN geom;
            ALTER TABLE africa.{table} RENAME COLUMN geom_clean TO geom;

            -- Spatial index
            CREATE INDEX idx_africa_{table}_geom ON africa.{table} USING GIST (geom);
            CREATE INDEX idx_africa_{table}_iso ON africa.{table} ("{iso_col}");
            ANALYZE africa.{table};
        """)

        # Count + validate
        result = run_sql(f"""
            SELECT
                COUNT(*) AS total,
                COUNT(*) FILTER (WHERE NOT ST_IsValid(geom)) AS invalid,
                COUNT(*) FILTER (WHERE ST_IsEmpty(geom)) AS empty
            FROM africa.{table};
        """)
        print(f"    Result: {result.stdout.strip()}")

        # Create GHA filtered table (dissolve by iso for admin0 to merge TZA+Zanzibar)
        print(f"  Creating gha.{table}...")
        if level == 0:
            run_sql(f"""
                DROP TABLE IF EXISTS gha.{table} CASCADE;
                CREATE TABLE gha.{table} AS
                SELECT
                    "{iso_col}",
                    MAX("country") AS country,
                    ST_Multi(ST_MakeValid(ST_Union(geom))) AS geom,
                    SUM(area_km2) AS area_km2
                FROM africa.{table}
                WHERE UPPER("{iso_col}") IN ({GHA_ISO3})
                GROUP BY "{iso_col}";

                CREATE INDEX idx_gha_{table}_geom ON gha.{table} USING GIST (geom);
                CREATE INDEX idx_gha_{table}_iso ON gha.{table} ("{iso_col}");
                ANALYZE gha.{table};
            """)
        else:
            run_sql(f"""
                DROP TABLE IF EXISTS gha.{table} CASCADE;
                CREATE TABLE gha.{table} AS
                SELECT *
                FROM africa.{table}
                WHERE UPPER("{iso_col}") IN ({GHA_ISO3});

                CREATE INDEX idx_gha_{table}_geom ON gha.{table} USING GIST (geom);
                CREATE INDEX idx_gha_{table}_iso ON gha.{table} ("{iso_col}");
                ANALYZE gha.{table};
            """)

        result = run_sql(f"SELECT COUNT(*) FROM gha.{table};")
        print(f"    GHA features: {result.stdout.strip()}")


def step3_create_baseline():
    """Create dissolved GHA baseline in PostGIS."""
    print("\n" + "=" * 60)
    print("  Step 3: Create dissolved GHA baseline")
    print("=" * 60)

    run_sql(f"""
        DROP TABLE IF EXISTS gha.baseline CASCADE;
        CREATE TABLE gha.baseline AS
        SELECT
            'GHA'::text AS region,
            COUNT(DISTINCT UPPER("{LEVELS[0]['iso_col']}"))::int AS country_count,
            ST_Multi(
                ST_MakeValid(
                    ST_Union(geom)
                )
            ) AS geom,
            ST_Multi(
                ST_MakeValid(
                    ST_SimplifyPreserveTopology(
                        ST_Union(geom),
                        {SIMPLIFY_TOLERANCE}
                    )
                )
            ) AS geom_simplified,
            ROUND(ST_Area(ST_Union(geom)::geography) / 1e6) AS area_km2
        FROM gha.admin0;

        CREATE INDEX idx_gha_baseline_geom ON gha.baseline USING GIST (geom);
        ANALYZE gha.baseline;
    """)

    result = run_sql("""
        SELECT country_count, area_km2::int,
               ST_NPoints(geom) AS vertices,
               ST_NPoints(geom_simplified) AS vertices_simplified
        FROM gha.baseline;
    """)
    print(f"  Baseline: {result.stdout.strip()}")


def step4_export():
    """Export from PostGIS to geojson + gpkg using ogr2ogr."""
    print("\n" + "=" * 60)
    print("  Step 4: Export to GeoJSON + GeoPackage")
    print("=" * 60)

    for schema in ["africa", "gha"]:
        out = OUT_DIR / schema
        out.mkdir(parents=True, exist_ok=True)

        for level in LEVELS:
            table = f"admin{level}"
            print(f"\n  Exporting {schema}.{table}...")

            # GeoPackage (full resolution)
            gpkg = out / f"{table}.gpkg"
            cmd = [
                "ogr2ogr", "-f", "GPKG", str(gpkg),
                OGR_PG_LOCAL,
                "-sql", f'SELECT * FROM "{schema}"."{table}"',
                "-nln", table,
                "-overwrite",
            ]
            subprocess.run(cmd, capture_output=True, text=True)
            if gpkg.exists():
                print(f"    {gpkg.name}: {gpkg.stat().st_size/1024/1024:.1f} MB")

            # GeoJSON (simplified in PostGIS, GHA only)
            if schema == "gha":
                gj = out / f"{table}.geojson"
                sql = (
                    f'SELECT *, ST_SimplifyPreserveTopology(geom, {SIMPLIFY_TOLERANCE}) AS geom_simp '
                    f'FROM "{schema}"."{table}"'
                )
                cmd = [
                    "ogr2ogr", "-f", "GeoJSON", str(gj),
                    OGR_PG_LOCAL,
                    "-sql", sql,
                    "-lco", "WRITE_BBOX=YES",
                    "-overwrite",
                ]
                subprocess.run(cmd, capture_output=True, text=True)
                if gj.exists():
                    print(f"    {gj.name}: {gj.stat().st_size/1024/1024:.1f} MB")

    # Baseline
    print("\n  Exporting gha.baseline...")
    for fmt, ext, extra in [
        ("GPKG", "gpkg", []),
        ("GeoJSON", "geojson", ["-lco", "WRITE_BBOX=YES"]),
    ]:
        out_file = OUT_DIR / "gha" / f"baseline.{ext}"
        cmd = [
            "ogr2ogr", "-f", fmt, str(out_file),
            OGR_PG_LOCAL,
            "-sql", 'SELECT region, country_count, area_km2, geom_simplified AS geom FROM gha.baseline',
            "-nln", "baseline",
            "-overwrite",
        ] + extra
        subprocess.run(cmd, capture_output=True, text=True)
        if out_file.exists():
            print(f"    baseline.{ext}: {out_file.stat().st_size/1024/1024:.1f} MB")


def step5_validate():
    """Final validation of exported files."""
    print("\n" + "=" * 60)
    print("  Step 5: Validate exports")
    print("=" * 60)

    for f in sorted(OUT_DIR.rglob("*.gpkg")):
        gdf = gpd.read_file(f)
        invalid = (~gdf.geometry.is_valid).sum()
        empty = gdf.geometry.is_empty.sum()
        status = "OK" if invalid == 0 and empty == 0 else f"WARN: {invalid} invalid, {empty} empty"
        print(f"  {f.relative_to(OUT_DIR)}: {len(gdf)} features — {status}")

    print(f"\n{'='*60}")
    print("  Output files:")
    print(f"{'='*60}")
    for f in sorted(OUT_DIR.rglob("*.*")):
        size_mb = f.stat().st_size / 1024 / 1024
        print(f"    {f.relative_to(OUT_DIR)} — {size_mb:.1f} MB")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Convert admin boundaries")
    parser.add_argument("--push", action="store_true", help="Push to remote PostGIS after")
    args = parser.parse_args()

    step1_load_to_postgis()
    step2_fix_topology_postgis()
    step3_create_baseline()
    step4_export()
    step5_validate()

    if args.push:
        print("\n  Pushing to remote PostGIS...")
        PG_REMOTE = "PG:host=149.102.153.66 port=5433 dbname=geodata user=geodata password=geodata"
        for schema in ["africa", "gha"]:
            for level in LEVELS:
                gpkg = OUT_DIR / schema / f"admin{level}.gpkg"
                cmd = [
                    "ogr2ogr", "-f", "PostgreSQL", PG_REMOTE,
                    str(gpkg), "-nln", f"{schema}.admin{level}",
                    "-overwrite", "-nlt", "PROMOTE_TO_MULTI",
                    "-lco", "GEOMETRY_NAME=geom",
                ]
                subprocess.run(cmd, capture_output=True, text=True)
                print(f"    {schema}.admin{level}")

        gpkg = OUT_DIR / "gha" / "baseline.gpkg"
        cmd = [
            "ogr2ogr", "-f", "PostgreSQL", PG_REMOTE,
            str(gpkg), "-nln", "gha.baseline",
            "-overwrite", "-lco", "GEOMETRY_NAME=geom",
        ]
        subprocess.run(cmd, capture_output=True, text=True)
        print(f"    gha.baseline")

    print("\n  DONE")
