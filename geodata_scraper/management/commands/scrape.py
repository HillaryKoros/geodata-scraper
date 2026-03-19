"""
Main scrape command — the ELT pipeline entry point.

Usage:
    # GADM for IGAD+ countries
    python manage.py scrape gadm --region igad_plus

    # GADM for specific countries
    python manage.py scrape gadm --countries KEN,TZA,UGA

    # GADM for all of Africa
    python manage.py scrape gadm --region africa

    # GADM globally (all countries)
    python manage.py scrape gadm --countries ALL

    # Specific format
    python manage.py scrape gadm --region igad --format geojson

    # Dry run
    python manage.py scrape gadm --region igad_plus --dry-run
"""

import logging

from django.core.management.base import BaseCommand

from geodata_scraper.models import DataSource, ScrapeJob, IngestedLayer
from geodata_scraper.scrapers import get_scraper
from geodata_scraper.regions import get_countries
from geodata_scraper.settings import scraper_settings
from geodata_scraper.ingest import load_gadm_gpkg, load_to_postgis
from geodata_scraper.ingest.transform import transform_layer, create_unified_view
from geodata_scraper.signals import layer_ingested, job_completed

log = logging.getLogger("geodata_scraper")


class Command(BaseCommand):
    help = "Run the geodata ELT pipeline: Extract → Load → Transform"

    def add_arguments(self, parser):
        parser.add_argument("source", type=str, help="Scraper name: gadm, http, api, ftp")
        parser.add_argument("--region", type=str, help="Region preset: igad, igad_plus, africa, etc.")
        parser.add_argument("--countries", type=str, help="Comma-separated ISO3 codes")
        parser.add_argument("--format", type=str, default="gpkg", help="Download format: gpkg, geojson, shp")
        parser.add_argument("--schema", type=str, help="PostGIS schema (default from settings)")
        parser.add_argument("--no-transform", action="store_true", help="Skip transform step")
        parser.add_argument("--no-views", action="store_true", help="Skip unified view creation")
        parser.add_argument("--dry-run", action="store_true", help="Show what would be done")
        parser.add_argument("--clean", action="store_true", help="Delete temp files after loading")

    def handle(self, *args, **options):
        source_name = options["source"]
        schema = options["schema"] or scraper_settings.DB_SCHEMA_RAW
        storage = scraper_settings.storage_path

        # Resolve countries
        if options["region"]:
            countries = get_countries(options["region"])
        elif options["countries"]:
            countries = get_countries(options["countries"])
        else:
            self.stderr.write("Error: Provide --region or --countries")
            return

        # Get scraper
        scraper_kwargs = {}
        if source_name == "gadm":
            scraper_kwargs["format"] = options["format"]
        scraper = get_scraper(source_name, **scraper_kwargs)

        # Build task list
        tasks = scraper.build_tasks(countries, storage_dir=storage)

        # Header
        self.stdout.write("=" * 60)
        self.stdout.write("  geodata-scraper ELT Pipeline")
        self.stdout.write("=" * 60)
        self.stdout.write(f"  Source    : {scraper.name} — {scraper.description}")
        self.stdout.write(f"  Countries : {', '.join(countries)} ({len(countries)})")
        self.stdout.write(f"  Format    : {options['format']}")
        self.stdout.write(f"  Schema    : {schema}")
        self.stdout.write(f"  Files     : {len(tasks)}")
        self.stdout.write("=" * 60)

        if options["dry_run"]:
            self.stdout.write("\n[DRY RUN] Would process:\n")
            for t in tasks:
                level = f"admin{t['admin_level']}" if t["admin_level"] >= 0 else "all-levels"
                self.stdout.write(f"  {t['iso3']} {level} → {t['dest']}")
            self.stdout.write(f"\nTotal: {len(tasks)} files")
            return

        # Create or get DataSource
        data_source, _ = DataSource.objects.get_or_create(
            name=f"GADM v4.1" if source_name == "gadm" else source_name,
            defaults={
                "source_type": source_name,
                "protocol": "https",
                "base_url": "https://geodata.ucdavis.edu/gadm/gadm4.1/",
            },
        )

        # Create job
        job = ScrapeJob.objects.create(
            source=data_source,
            region=options.get("region", ""),
            countries=countries,
            total_files=len(tasks),
        )
        job.start()

        # ── EXTRACT ──────────────────────────────────────────────
        self.stdout.write("\n[1/3] EXTRACT — downloading...")
        results = scraper.extract_all(countries, storage, format=options["format"])

        ok = [r for r in results if r.success]
        failed = [r for r in results if not r.success]
        total_bytes = sum(r.size for r in ok)

        job.downloaded_files = len(ok)
        job.failed_files = len(failed)
        job.bytes_downloaded = total_bytes
        job.status = "loading"
        job.save()

        for r in sorted(ok, key=lambda x: x.iso3):
            self.stdout.write(f"  OK  {r.iso3:3s}  {r.local_path.name:35s}  {_fmt(r.size)}")
        self.stdout.write(f"\n  Total: {len(ok)}/{len(results)} files ({_fmt(total_bytes)})")
        if failed:
            for f in failed:
                self.stderr.write(f"  FAILED: {f.url} — {f.error}")

        # ── LOAD ─────────────────────────────────────────────────
        self.stdout.write("\n[2/3] LOAD — ingesting into PostGIS...")
        loaded_count = 0

        for result in sorted(ok, key=lambda x: x.iso3):
            self.stdout.write(f"\n  ▶ {result.iso3} — loading {result.local_path.name}...")
            try:
                if source_name == "gadm" and result.format == "gpkg":
                    metas = load_gadm_gpkg(result.local_path, result.iso3, schema)
                else:
                    table_name = f"{result.iso3.lower()}_admin{result.admin_level}"
                    meta = load_to_postgis(
                        result.local_path, schema, table_name,
                    )
                    meta["iso3"] = result.iso3
                    meta["admin_level"] = result.admin_level
                    metas = [meta]

                for meta in metas:
                    if "error" not in meta:
                        layer = IngestedLayer.objects.update_or_create(
                            db_schema=meta["schema"],
                            db_table=meta["table"],
                            defaults={
                                "job": job,
                                "source": data_source,
                                "name": f"{result.iso3} Admin Level {meta.get('admin_level', 0)}",
                                "iso3": meta.get("iso3", result.iso3),
                                "admin_level": meta.get("admin_level"),
                                "feature_count": meta.get("feature_count", 0),
                                "properties": meta.get("columns", []),
                                "geom_column": "geom",
                                "srid": 4326,
                                "source_url": result.url,
                                "source_format": result.format,
                                "file_size": result.size,
                            },
                        )[0]
                        loaded_count += 1
                        layer_ingested.send(sender=layer.__class__, instance=layer)
                        lvl = meta.get('admin_level', '?')
                        self.stdout.write(
                            f"    admin{lvl}: {meta['schema']}.{meta['table']} "
                            f"— {meta.get('feature_count', 0)} features"
                        )

            except Exception as e:
                self.stderr.write(f"  LOAD ERROR: {result.local_path.name} — {e}")

        job.loaded_tables = loaded_count
        job.status = "transforming"
        job.save()

        # ── TRANSFORM ────────────────────────────────────────────
        if not options["no_transform"]:
            self.stdout.write("\n[3/3] TRANSFORM — normalizing in PostGIS...")
            for layer in job.layers.all():
                try:
                    result = transform_layer(layer.db_schema, layer.db_table)
                    for t in result["transforms"]:
                        self.stdout.write(f"  {layer.db_table}: {t}")
                except Exception as e:
                    self.stderr.write(f"  TRANSFORM ERROR: {layer.db_table} — {e}")

            # Create unified views
            if not options["no_views"]:
                try:
                    create_unified_view(
                        schema,
                        scraper_settings.DB_SCHEMA_CLEAN,
                        countries,
                    )
                    self.stdout.write(f"  Unified views created in {scraper_settings.DB_SCHEMA_CLEAN}")
                except Exception as e:
                    self.stderr.write(f"  VIEW ERROR: {e}")
        else:
            self.stdout.write("\n[3/3] TRANSFORM — skipped")

        # ── CLEANUP ──────────────────────────────────────────────
        if options["clean"]:
            import shutil
            gadm_dir = storage / "gadm"
            if gadm_dir.exists():
                shutil.rmtree(gadm_dir)
                self.stdout.write("  Temp files cleaned")

        # ── DONE ─────────────────────────────────────────────────
        job.complete()
        job_completed.send(sender=job.__class__, instance=job)

        self.stdout.write("\n" + "=" * 60)
        self.stdout.write("  DONE")
        self.stdout.write("=" * 60)
        self.stdout.write(f"  Downloaded : {job.downloaded_files} files ({_fmt(job.bytes_downloaded)})")
        self.stdout.write(f"  Loaded     : {job.loaded_tables} tables")
        self.stdout.write(f"  Failed     : {job.failed_files}")
        self.stdout.write(f"  Schema     : {schema} (raw), {scraper_settings.DB_SCHEMA_CLEAN} (clean)")
        self.stdout.write(f"  Duration   : {job.duration}")
        self.stdout.write("=" * 60)


def _fmt(n):
    for u in ("B", "KB", "MB", "GB"):
        if abs(n) < 1024:
            return f"{n:.1f} {u}"
        n /= 1024
    return f"{n:.1f} TB"
