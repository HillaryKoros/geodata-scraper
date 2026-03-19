"""
Scrape HydroSHEDS data — basins and rivers clipped to IGAD+ region.

Downloads Africa-wide HydroBASINS + HydroRIVERS, then clips to the
combined simplified admin0 boundary of IGAD+ countries.

Usage:
    # Default: basins levels 4,6,8 + rivers, clipped to IGAD+
    python manage.py scrape_hydrosheds

    # All 12 levels
    python manage.py scrape_hydrosheds --levels 1,2,3,4,5,6,7,8,9,10,11,12

    # No clipping (full Africa)
    python manage.py scrape_hydrosheds --no-clip

    # Dry run
    python manage.py scrape_hydrosheds --dry-run
"""

import logging
from pathlib import Path

from django.core.management.base import BaseCommand
from django.db import connection

from geodata_scraper.scrapers import get_scraper
from geodata_scraper.models import DataSource, ScrapeJob, IngestedLayer
from geodata_scraper.settings import scraper_settings
from geodata_scraper.ingest import load_to_postgis
from geodata_scraper.ingest.transform import transform_layer

log = logging.getLogger("geodata_scraper")


class Command(BaseCommand):
    help = "Scrape HydroSHEDS basins + rivers, clip to IGAD+ admin0"

    def add_arguments(self, parser):
        parser.add_argument("--levels", type=str, default="1,2,3,4,5,6,7,8,9,10,11,12", help="Basin levels (comma-separated, default: all 12)")
        parser.add_argument("--no-rivers", action="store_true", help="Skip HydroRIVERS")
        parser.add_argument("--no-clip", action="store_true", help="Keep full Africa (no clip to IGAD+)")
        parser.add_argument("--schema", type=str, default="hydro_raw")
        parser.add_argument("--dry-run", action="store_true")
        parser.add_argument("--continent", type=str, default="africa")

    def handle(self, *args, **options):
        levels = [int(x) for x in options["levels"].split(",")]
        schema = options["schema"]
        storage = scraper_settings.storage_path
        include_rivers = not options["no_rivers"]
        clip = not options["no_clip"]

        scraper = get_scraper(
            "hydrosheds",
            continent=options["continent"],
            levels=levels,
            include_rivers=include_rivers,
        )

        tasks = scraper.build_tasks(storage_dir=storage)

        self.stdout.write("=" * 60)
        self.stdout.write("  HydroSHEDS ELT Pipeline")
        self.stdout.write("=" * 60)
        self.stdout.write(f"  Continent : {options['continent']}")
        self.stdout.write(f"  Basins    : levels {levels}")
        self.stdout.write(f"  Rivers    : {'yes' if include_rivers else 'no'}")
        self.stdout.write(f"  Clip IGAD+: {'yes' if clip else 'no (full Africa)'}")
        self.stdout.write(f"  Schema    : {schema}")
        self.stdout.write(f"  Files     : {len(tasks)}")
        self.stdout.write("=" * 60)

        if options["dry_run"]:
            self.stdout.write("\n[DRY RUN]\n")
            for t in tasks:
                ltype = t.get("layer_type", "basins")
                lvl = f"lev{t['admin_level']:02d}" if t["admin_level"] >= 0 else "all"
                self.stdout.write(f"  {ltype:8s} {lvl:6s} → {t['dest']}")
            return

        # Ensure schema
        with connection.cursor() as cur:
            cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')

        # Create source
        data_source, _ = DataSource.objects.get_or_create(
            name="HydroSHEDS v1.0",
            defaults={"source_type": "generic", "protocol": "https",
                       "base_url": "https://data.hydrosheds.org/"},
        )

        job = ScrapeJob.objects.create(
            source=data_source, region=options["continent"],
            countries=[], total_files=len(tasks),
        )
        job.start()

        # ── EXTRACT ──────────────────────────────────────────
        self.stdout.write("\n[1/3] EXTRACT — downloading HydroSHEDS...")
        results = scraper.extract_all([], storage)

        ok = [r for r in results if r.success]
        failed = [r for r in results if not r.success]
        total_bytes = sum(r.size for r in ok)

        for r in ok:
            lvl = f"lev{r.admin_level:02d}" if r.admin_level >= 0 else "rivers"
            self.stdout.write(f"  OK  {lvl:8s}  {r.local_path.name:45s}  {_fmt(r.size)}")

        self.stdout.write(f"\n  Total: {len(ok)}/{len(results)} ({_fmt(total_bytes)})")
        for f in failed:
            self.stderr.write(f"  FAILED: {f.error}")

        job.downloaded_files = len(ok)
        job.failed_files = len(failed)
        job.bytes_downloaded = total_bytes
        job.status = "loading"
        job.save()

        # ── LOAD ─────────────────────────────────────────────
        self.stdout.write("\n[2/3] LOAD — ingesting into PostGIS...")
        loaded = 0

        for result in ok:
            # Find the .shp file inside extracted directory
            shp_files = list(result.local_path.rglob("*.shp"))
            if not shp_files:
                self.stderr.write(f"  No .shp found in {result.local_path}")
                continue

            for shp in shp_files:
                if result.admin_level >= 0:
                    table_name = f"hydrobasins_af_lev{result.admin_level:02d}"
                else:
                    table_name = f"hydrorivers_af"

                self.stdout.write(f"\n  ▶ {shp.name} → {schema}.{table_name}")

                try:
                    meta = load_to_postgis(
                        file_path=shp,
                        schema=schema,
                        table_name=table_name,
                    )

                    feat_count = meta.get("feature_count", 0)
                    self.stdout.write(f"    Loaded: {feat_count} features")

                    # Clip to IGAD+ if requested
                    if clip:
                        clipped = self._clip_to_igad(schema, table_name)
                        if clipped is not None:
                            self.stdout.write(f"    Clipped to IGAD+: {clipped} features remaining")
                            feat_count = clipped

                    IngestedLayer.objects.update_or_create(
                        db_schema=schema, db_table=table_name,
                        defaults={
                            "job": job, "source": data_source,
                            "name": f"HydroBASINS Africa lev{result.admin_level:02d}" if result.admin_level >= 0 else "HydroRIVERS Africa",
                            "iso3": "AF",
                            "admin_level": result.admin_level if result.admin_level >= 0 else None,
                            "feature_count": feat_count,
                            "properties": meta.get("columns", []),
                            "geom_column": "geom", "srid": 4326,
                            "source_url": result.url,
                            "source_format": "shp",
                            "file_size": result.size,
                        },
                    )
                    loaded += 1

                except Exception as e:
                    self.stderr.write(f"    ERROR: {e}")

        job.loaded_tables = loaded
        job.status = "transforming"
        job.save()

        # ── TRANSFORM ────────────────────────────────────────
        self.stdout.write("\n[3/3] TRANSFORM — indexing...")
        for layer in job.layers.all():
            try:
                result = transform_layer(layer.db_schema, layer.db_table)
                for t in result["transforms"]:
                    self.stdout.write(f"    {layer.db_table}: {t}")
            except Exception as e:
                self.stderr.write(f"    TRANSFORM ERROR: {layer.db_table} — {e}")

        job.complete()

        self.stdout.write("\n" + "=" * 60)
        self.stdout.write("  DONE — HydroSHEDS")
        self.stdout.write("=" * 60)
        self.stdout.write(f"  Downloaded : {job.downloaded_files} files ({_fmt(job.bytes_downloaded)})")
        self.stdout.write(f"  Loaded     : {job.loaded_tables} tables")
        self.stdout.write(f"  Schema     : {schema}")
        self.stdout.write(f"  Duration   : {job.duration}")
        self.stdout.write("=" * 60)

    def _clip_to_igad(self, schema: str, table_name: str) -> int | None:
        """
        Clip a HydroSHEDS table to the combined IGAD+ admin0 boundary.
        Uses ST_Intersects against geodata_raw admin0 tables.
        """
        igad_countries = ["dji", "eri", "eth", "ken", "som", "ssd", "sdn", "uga", "bdi", "rwa", "tza"]
        raw_schema = scraper_settings.DB_SCHEMA_RAW

        try:
            with connection.cursor() as cur:
                # Check if GADM admin0 tables exist
                existing = []
                for iso3 in igad_countries:
                    cur.execute("""
                        SELECT table_name FROM information_schema.tables
                        WHERE table_schema = %s AND table_name = %s
                    """, [raw_schema, f"{iso3}_admin0"])
                    if cur.fetchone():
                        existing.append(iso3)

                if not existing:
                    log.warning("No GADM admin0 tables found — skipping clip")
                    return None

                # Build combined IGAD+ boundary
                union_parts = [
                    f'SELECT ST_Union(geom) AS geom FROM "{raw_schema}"."{iso3}_admin0"'
                    for iso3 in existing
                ]
                union_sql = " UNION ALL ".join(union_parts)

                # Delete features outside IGAD+
                cur.execute(f"""
                    DELETE FROM "{schema}"."{table_name}" t
                    WHERE NOT EXISTS (
                        SELECT 1 FROM ({union_sql}) igad
                        WHERE ST_Intersects(t.geom, igad.geom)
                    )
                """)
                deleted = cur.rowcount

                # Get remaining count
                cur.execute(f'SELECT COUNT(*) FROM "{schema}"."{table_name}"')
                remaining = cur.fetchone()[0]

                log.info(f"Clipped {table_name}: removed {deleted}, kept {remaining}")
                return remaining

        except Exception as e:
            log.warning(f"Clip failed for {table_name}: {e}")
            return None


def _fmt(n):
    for u in ("B", "KB", "MB", "GB"):
        if abs(n) < 1024:
            return f"{n:.1f} {u}"
        n /= 1024
    return f"{n:.1f} TB"
