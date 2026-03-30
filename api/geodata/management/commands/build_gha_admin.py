"""
Build merged GHA (Greater Horn of Africa) admin boundary layers.

Creates unified admin 0-3 tables for the IGAD+ region in the 'gha' schema,
with topology correction, simplification, and spatial indexes.

Usage:
    python manage.py build_gha_admin
    python manage.py build_gha_admin --simplify 0.005
    python manage.py build_gha_admin --levels 0 1 2
"""

import logging

from django.core.management.base import BaseCommand
from django.db import connection

from geodata.settings import scraper_settings

log = logging.getLogger("geodata")

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

# Common columns per admin level (GADM standard)
LEVEL_COLUMNS = {
    0: {
        "id": "gid_0",
        "name": "country",
        "extra": [],
    },
    1: {
        "id": "gid_1",
        "name": "name_1",
        "extra": ["gid_0", "country", "type_1", "engtype_1"],
    },
    2: {
        "id": "gid_2",
        "name": "name_2",
        "extra": ["gid_0", "gid_1", "country", "name_1", "type_2", "engtype_2"],
    },
    3: {
        "id": "gid_3",
        "name": "name_3",
        "extra": [
            "gid_0",
            "gid_1",
            "gid_2",
            "country",
            "name_1",
            "name_2",
            "type_3",
            "engtype_3",
        ],
    },
}


class Command(BaseCommand):
    help = "Build merged GHA admin boundary layers (admin 0-3) with topology correction"

    def add_arguments(self, parser):
        parser.add_argument(
            "--simplify",
            type=float,
            default=0.001,
            help="Simplification tolerance in degrees (default: 0.001 ~100m)",
        )
        parser.add_argument(
            "--levels",
            nargs="+",
            type=int,
            default=[0, 1, 2, 3],
            help="Admin levels to build (default: 0 1 2 3)",
        )
        parser.add_argument(
            "--schema",
            type=str,
            default="gha",
            help="Output schema (default: gha)",
        )
        parser.add_argument(
            "--raw-schema",
            type=str,
            help="Schema with raw admin tables",
        )

    def handle(self, *args, **options):
        raw_schema = options["raw_schema"] or scraper_settings.DB_SCHEMA_RAW
        out_schema = options["schema"]
        simplify = options["simplify"]
        levels = options["levels"]

        self.stdout.write("=" * 60)
        self.stdout.write("  Building GHA Region Admin Boundaries")
        self.stdout.write("=" * 60)

        with connection.cursor() as cur:
            cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{out_schema}"')

            for level in levels:
                self._build_level(cur, raw_schema, out_schema, level, simplify)

            # Build dissolved region baseline
            self._build_baseline(cur, out_schema, simplify)

        self.stdout.write("\n" + "=" * 60)
        self.stdout.write("  DONE — GHA Admin Boundaries Created")
        self.stdout.write("=" * 60)

    def _build_level(self, cur, raw_schema, out_schema, level, simplify):
        table_name = f"admin{level}"
        self.stdout.write(f"\n  --- Admin Level {level} ---")

        # Find which countries have this admin level
        available = []
        for iso3 in IGAD_COUNTRIES:
            cur.execute(
                """
                SELECT table_name FROM information_schema.tables
                WHERE table_schema = %s AND table_name = %s
            """,
                [raw_schema, f"{iso3}_admin{level}"],
            )
            if cur.fetchone():
                available.append(iso3)

        if not available:
            self.stdout.write(f"    SKIP: No admin{level} tables found")
            return

        self.stdout.write(
            f"    Countries: {', '.join(c.upper() for c in available)} ({len(available)})"
        )

        # Get columns that exist in the raw tables
        cols = LEVEL_COLUMNS.get(level, LEVEL_COLUMNS[0])
        id_col = cols["id"]
        name_col = cols["name"]
        extra_cols = cols["extra"]

        # Check which extra columns actually exist in first available table
        cur.execute(
            """
            SELECT column_name FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
        """,
            [raw_schema, f"{available[0]}_admin{level}"],
        )
        existing_cols = {row[0] for row in cur.fetchall()}
        extra_cols = [c for c in extra_cols if c in existing_cols]

        # Build select columns
        select_cols = [f'"{id_col}"', f'"{name_col}"']
        for col in extra_cols:
            select_cols.append(f'"{col}"')

        select_str = ", ".join(select_cols)

        # Union all countries
        union_parts = []
        for iso3 in available:
            union_parts.append(
                f"SELECT '{iso3.upper()}' AS iso3, {select_str}, geom "
                f'FROM "{raw_schema}"."{iso3}_admin{level}"'
            )
        union_sql = " UNION ALL ".join(union_parts)

        # Create merged table with topology correction
        cur.execute(f'DROP TABLE IF EXISTS "{out_schema}"."{table_name}" CASCADE')
        cur.execute(f"""
            CREATE TABLE "{out_schema}"."{table_name}" AS
            SELECT
                iso3,
                {select_str},
                ST_MakeValid(geom) AS geom,
                ST_MakeValid(ST_SimplifyPreserveTopology(geom, {simplify})) AS geom_simplified,
                ST_Area(geom::geography) / 1e6 AS area_km2
            FROM ({union_sql}) t
            WHERE geom IS NOT NULL
        """)

        # Count features
        cur.execute(f'SELECT COUNT(*) FROM "{out_schema}"."{table_name}"')
        count = cur.fetchone()[0]

        # Get vertex stats
        cur.execute(f"""
            SELECT
                SUM(ST_NPoints(geom)),
                SUM(ST_NPoints(geom_simplified))
            FROM "{out_schema}"."{table_name}"
        """)
        orig_pts, simp_pts = cur.fetchone()
        reduction = (1 - simp_pts / orig_pts) * 100 if orig_pts > 0 else 0

        # Spatial indexes
        cur.execute(f"""
            CREATE INDEX IF NOT EXISTS "idx_{table_name}_geom"
            ON "{out_schema}"."{table_name}" USING GIST (geom)
        """)
        cur.execute(f"""
            CREATE INDEX IF NOT EXISTS "idx_{table_name}_geom_simplified"
            ON "{out_schema}"."{table_name}" USING GIST (geom_simplified)
        """)
        cur.execute(f"""
            CREATE INDEX IF NOT EXISTS "idx_{table_name}_iso3"
            ON "{out_schema}"."{table_name}" (iso3)
        """)
        cur.execute(f'ANALYZE "{out_schema}"."{table_name}"')

        self.stdout.write(f"    {out_schema}.{table_name}: {count} features")
        self.stdout.write(
            f"    Vertices: {orig_pts:,} -> {simp_pts:,} ({reduction:.0f}% reduction)"
        )

    def _build_baseline(self, cur, out_schema, simplify):
        """Create dissolved region outline from admin0."""
        self.stdout.write("\n  --- Region Baseline (dissolved) ---")

        cur.execute(
            """
            SELECT COUNT(*) FROM information_schema.tables
            WHERE table_schema = %s AND table_name = 'admin0'
        """,
            [out_schema],
        )
        if not cur.fetchone()[0]:
            self.stdout.write("    SKIP: admin0 not built yet")
            return

        cur.execute(f'DROP TABLE IF EXISTS "{out_schema}"."baseline" CASCADE')
        cur.execute(f"""
            CREATE TABLE "{out_schema}"."baseline" AS
            SELECT
                'GHA' AS region,
                COUNT(DISTINCT iso3)::int AS country_count,
                ST_MakeValid(ST_Union(geom)) AS geom,
                ST_MakeValid(
                    ST_SimplifyPreserveTopology(ST_Union(geom), {simplify})
                ) AS geom_simplified,
                ST_Area(ST_Union(geom)::geography) / 1e6 AS area_km2
            FROM "{out_schema}"."admin0"
        """)

        cur.execute(f"""
            CREATE INDEX IF NOT EXISTS "idx_baseline_geom"
            ON "{out_schema}"."baseline" USING GIST (geom)
        """)
        cur.execute(f'ANALYZE "{out_schema}"."baseline"')

        cur.execute(f"""
            SELECT country_count, area_km2::int,
                   ST_NPoints(geom), ST_NPoints(geom_simplified)
            FROM "{out_schema}"."baseline"
        """)
        countries, area, orig, simp = cur.fetchone()
        self.stdout.write(f"    {out_schema}.baseline: 1 dissolved polygon")
        self.stdout.write(f"    {countries} countries, {area:,} km2")
        self.stdout.write(f"    Vertices: {orig:,} -> {simp:,}")
