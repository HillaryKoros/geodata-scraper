from django.apps import AppConfig
from django.db import connection


class GeodataScraperConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "geodata_scraper"
    verbose_name = "Geodata Scraper"

    def ready(self):
        from . import signals  # noqa: F401
        self._ensure_schemas()

    def _ensure_schemas(self):
        """Create raw and clean PostGIS schemas on startup."""
        from .settings import scraper_settings

        try:
            with connection.cursor() as cur:
                for schema in (scraper_settings.DB_SCHEMA_RAW, scraper_settings.DB_SCHEMA_CLEAN):
                    cur.execute(
                        "SELECT schema_name FROM information_schema.schemata WHERE schema_name = %s",
                        [schema],
                    )
                    if not cur.fetchone():
                        cur.execute(f'CREATE SCHEMA "{schema}"')
        except Exception:
            # DB not ready yet (e.g. during migrations) — skip
            pass
