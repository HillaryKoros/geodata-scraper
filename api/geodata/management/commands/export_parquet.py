"""
Export ingested boundaries as Parquet files — merged per admin level.

Usage:
    # Export all admin levels as merged parquet
    python manage.py export_parquet --region igad_plus --output /data/parquet

    # Specific level
    python manage.py export_parquet --region igad_plus --level 0 --simplify 0.01
"""

import logging
from pathlib import Path

from django.core.management.base import BaseCommand
from django.db import connection

from geodata.regions import get_countries, get_admin_levels
from geodata.settings import scraper_settings

log = logging.getLogger("geodata")


class Command(BaseCommand):
    help = "Export merged boundaries as GeoParquet files (one per admin level)"

    def add_arguments(self, parser):
        parser.add_argument("--region", type=str, default="igad_plus")
        parser.add_argument("--countries", type=str, help="Comma-separated ISO3")
        parser.add_argument("--output", type=str, default="/data/parquet")
        parser.add_argument(
            "--level", type=int, help="Specific admin level (default: all)"
        )
        parser.add_argument(
            "--simplify", type=float, default=0, help="Simplify tolerance (degrees)"
        )
        parser.add_argument("--schema", type=str, help="PostGIS schema")

    def handle(self, *args, **options):
        import geopandas as gpd

        if options["countries"]:
            countries = get_countries(options["countries"])
        else:
            countries = get_countries(options["region"])

        schema = options["schema"] or scraper_settings.DB_SCHEMA_RAW
        output_dir = Path(options["output"])
        output_dir.mkdir(parents=True, exist_ok=True)
        simplify = options["simplify"]

        max_level = max(get_admin_levels(c) for c in countries)
        levels = (
            [options["level"]] if options["level"] is not None else range(max_level + 1)
        )

        self.stdout.write("=" * 60)
        self.stdout.write("  Parquet Export — Merged Admin Boundaries")
        self.stdout.write("=" * 60)
        self.stdout.write(f"  Countries : {', '.join(countries)}")
        self.stdout.write(f"  Levels    : {list(levels)}")
        self.stdout.write(f"  Output    : {output_dir}")
        if simplify:
            self.stdout.write(f"  Simplify  : {simplify} degrees")
        self.stdout.write("=" * 60)

        for level in levels:
            self.stdout.write(f"\n  Admin Level {level}...")

            # Build UNION ALL query across all countries for this level
            parts = []
            for iso3 in countries:
                if level > get_admin_levels(iso3):
                    continue
                table = f"{iso3.lower()}_admin{level}"
                # Check table exists
                with connection.cursor() as cur:
                    cur.execute(
                        """
                        SELECT table_name FROM information_schema.tables
                        WHERE table_schema = %s AND table_name = %s
                    """,
                        [schema, table],
                    )
                    if cur.fetchone():
                        geom = "geom"
                        if simplify > 0:
                            geom = f"ST_Simplify(geom, {simplify})"
                        parts.append(
                            f"SELECT *, '{iso3.upper()}' AS country_iso3, "
                            f"{geom} AS geometry "
                            f'FROM "{schema}"."{table}"'
                        )

            if not parts:
                self.stdout.write(f"    No data for admin level {level}")
                continue

            sql = " UNION ALL ".join(parts)

            try:
                gdf = gpd.read_postgis(
                    sql,
                    connection.cursor().connection,
                    geom_col="geometry",
                )

                # Drop duplicate geom column if present
                if "geom" in gdf.columns and "geometry" in gdf.columns:
                    gdf = gdf.drop(columns=["geom"])

                # Export
                filename = f"igad_admin{level}.parquet"
                filepath = output_dir / filename
                gdf.to_parquet(filepath, index=False)

                size_mb = filepath.stat().st_size / (1024 * 1024)
                self.stdout.write(
                    f"    OK: {filename} — {len(gdf)} features, {size_mb:.1f} MB"
                )

            except Exception as e:
                self.stderr.write(f"    ERROR level {level}: {e}")

        self.stdout.write(f"\n  Parquet files: {output_dir}")
        self.stdout.write("=" * 60)
