"""
Default settings for geodata_scraper.

Override in your Django settings.py:

    GEODATA_SCRAPER = {
        "DB_SCHEMA_RAW": "gadm_raw",
        "DB_SCHEMA_CLEAN": "geodata",
        ...
    }
"""

from dataclasses import dataclass
from pathlib import Path

from django.conf import settings


DEFAULTS = {
    "DB_SCHEMA_RAW": "geodata_raw",
    "DB_SCHEMA_CLEAN": "geodata",
    "STORAGE_DIR": "/tmp/geodata_scraper",
    "DEFAULT_SRID": 4326,
    "OGR2OGR_BIN": "ogr2ogr",
    "DOWNLOAD_TIMEOUT": 300,
    "DOWNLOAD_RETRIES": 3,
    "DOWNLOAD_WORKERS": 3,
    "CHUNK_SIZE": 256 * 1024,  # 256KB
}


@dataclass
class ScraperSettings:
    DB_SCHEMA_RAW: str = DEFAULTS["DB_SCHEMA_RAW"]
    DB_SCHEMA_CLEAN: str = DEFAULTS["DB_SCHEMA_CLEAN"]
    STORAGE_DIR: str = DEFAULTS["STORAGE_DIR"]
    DEFAULT_SRID: int = DEFAULTS["DEFAULT_SRID"]
    OGR2OGR_BIN: str = DEFAULTS["OGR2OGR_BIN"]
    DOWNLOAD_TIMEOUT: int = DEFAULTS["DOWNLOAD_TIMEOUT"]
    DOWNLOAD_RETRIES: int = DEFAULTS["DOWNLOAD_RETRIES"]
    DOWNLOAD_WORKERS: int = DEFAULTS["DOWNLOAD_WORKERS"]
    CHUNK_SIZE: int = DEFAULTS["CHUNK_SIZE"]

    @property
    def storage_path(self) -> Path:
        p = Path(self.STORAGE_DIR)
        p.mkdir(parents=True, exist_ok=True)
        return p


def get_settings() -> ScraperSettings:
    user = getattr(settings, "GEODATA_SCRAPER", {})
    merged = {**DEFAULTS, **user}
    return ScraperSettings(**merged)


scraper_settings = get_settings()
