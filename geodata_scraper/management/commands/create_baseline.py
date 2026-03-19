"""
Create the IGAD+ baseline boundary — merged + dissolved + simplified admin0.

This is the foundation geometry used to clip all other datasets
(HydroSHEDS, OSM, rasters, etc.)

Usage:
    python manage.py create_baseline
    python manage.py create_baseline --simplify 0.005
"""

import logging
from django.core.management.base import BaseCommand
from django.db import connection

from geodata_scraper.settings import scraper_settings

log = logging.getLogger("geodata_scraper")

IGAD_COUNTRIES = [
    "dji",
    "eri",
    "eth",
    "ken",
    "som",
    "ssd",
    "sdn",
    "uga",
    "bdi",
    "rwa",
    "tza",
]


class Command(BaseCommand):
    help = "Create IGAD+ merged/dissolved/simplified admin0 baseline boundary"

    def add_arguments(self, parser):
        parser.add_argument(
            "--simplify",
            type=float,
            default=0.01,
            help="Simplification tolerance in degrees (default: 0.01 ~1km)",
        )
        parser.add_argument("--schema", type=str, help="Schema with admin0 tables")
        parser.add_argument(
            "--output-schema",
            type=str,
            default="geodata",
            help="Schema for baseline output",
        )

    def handle(self, *args, **options):
        raw_schema = options["schema"] or scraper_settings.DB_SCHEMA_RAW
        out_schema = options["output_schema"]
        simplify = options["simplify"]

        self.stdout.write("=" * 60)
        self.stdout.write("  Creating IGAD+ Baseline Boundary")
        self.stdout.write("=" * 60)

        with connection.cursor() as cur:
            cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{out_schema}"')

            # Check which countries have admin0 loaded
            available = []
            for iso3 in IGAD_COUNTRIES:
                cur.execute(
                    """
                    SELECT table_name FROM information_schema.tables
                    WHERE table_schema = %s AND table_name = %s
                """,
                    [raw_schema, f"{iso3}_admin0"],
                )
                if cur.fetchone():
                    available.append(iso3)

            if not available:
                self.stderr.write(
                    "  ERROR: No admin0 tables found. Run GADM scrape first."
                )
                return

            self.stdout.write(
                f"  Countries found: {', '.join(c.upper() for c in available)} ({len(available)}/11)"
            )

            missing = set(IGAD_COUNTRIES) - set(available)
            if missing:
                self.stdout.write(f"  Missing: {', '.join(c.upper() for c in missing)}")

            # Step 1: Merge all admin0 into one table (individual countries)
            self.stdout.write("\n  [1/4] Merging admin0 polygons...")
            union_parts = []
            for iso3 in available:
                union_parts.append(
                    f'SELECT \'{iso3.upper()}\' AS iso3, geom FROM "{raw_schema}"."{iso3}_admin0"'
                )

            union_sql = " UNION ALL ".join(union_parts)

            cur.execute(
                f'DROP TABLE IF EXISTS "{out_schema}"."igad_admin0_merged" CASCADE'
            )
            cur.execute(f'''
                CREATE TABLE "{out_schema}"."igad_admin0_merged" AS
                SELECT iso3, geom FROM ({union_sql}) t
            ''')
            cur.execute(f'SELECT COUNT(*) FROM "{out_schema}"."igad_admin0_merged"')
            count = cur.fetchone()[0]
            self.stdout.write(f"    igad_admin0_merged: {count} country polygons")

            # Step 2: Dissolve into single geometry (the IGAD+ region outline)
            self.stdout.write("  [2/4] Dissolving into single region boundary...")
            cur.execute(f'DROP TABLE IF EXISTS "{out_schema}"."igad_baseline" CASCADE')
            cur.execute(f'''
                CREATE TABLE "{out_schema}"."igad_baseline" AS
                SELECT
                    'IGAD_PLUS' AS region,
                    {len(available)} AS country_count,
                    ST_Union(geom) AS geom
                FROM "{out_schema}"."igad_admin0_merged"
            ''')
            self.stdout.write("    igad_baseline: 1 dissolved polygon")

            # Step 3: Simplified version
            self.stdout.write(f"  [3/4] Simplifying (tolerance: {simplify} degrees)...")
            cur.execute(
                f'DROP TABLE IF EXISTS "{out_schema}"."igad_baseline_simplified" CASCADE'
            )
            cur.execute(f'''
                CREATE TABLE "{out_schema}"."igad_baseline_simplified" AS
                SELECT
                    region,
                    country_count,
                    ST_Simplify(geom, {simplify}) AS geom
                FROM "{out_schema}"."igad_baseline"
            ''')

            # Get sizes for comparison
            cur.execute(f'''
                SELECT
                    ST_NPoints((SELECT geom FROM "{out_schema}"."igad_baseline")),
                    ST_NPoints((SELECT geom FROM "{out_schema}"."igad_baseline_simplified")),
                    ST_Area((SELECT geom::geography FROM "{out_schema}"."igad_baseline")) / 1e6
            ''')
            orig_pts, simp_pts, area_km2 = cur.fetchone()
            reduction = (1 - simp_pts / orig_pts) * 100 if orig_pts > 0 else 0
            self.stdout.write(f"    Original: {orig_pts:,} vertices")
            self.stdout.write(
                f"    Simplified: {simp_pts:,} vertices ({reduction:.0f}% reduction)"
            )
            self.stdout.write(f"    Total area: {area_km2:,.0f} km2")

            # Step 4: Spatial indexes
            self.stdout.write("  [4/4] Creating spatial indexes...")
            for tbl in [
                "igad_admin0_merged",
                "igad_baseline",
                "igad_baseline_simplified",
            ]:
                cur.execute(f'''
                    CREATE INDEX IF NOT EXISTS "idx_{tbl}_geom"
                    ON "{out_schema}"."{tbl}" USING GIST (geom)
                ''')
            cur.execute(f'ANALYZE "{out_schema}"."igad_admin0_merged"')
            cur.execute(f'ANALYZE "{out_schema}"."igad_baseline"')
            cur.execute(f'ANALYZE "{out_schema}"."igad_baseline_simplified"')

        self.stdout.write("\n" + "=" * 60)
        self.stdout.write("  DONE — Baseline Tables Created")
        self.stdout.write("=" * 60)
        self.stdout.write(
            f"  {out_schema}.igad_admin0_merged      — {count} country polygons"
        )
        self.stdout.write(
            f"  {out_schema}.igad_baseline            — 1 dissolved IGAD+ outline"
        )
        self.stdout.write(
            f"  {out_schema}.igad_baseline_simplified — 1 simplified outline"
        )
        self.stdout.write(
            "\n  Use igad_baseline_simplified for clipping HydroSHEDS, OSM, etc."
        )
        self.stdout.write("=" * 60)
