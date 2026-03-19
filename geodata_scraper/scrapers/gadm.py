"""
GADM scraper — downloads admin boundary data from GADM v4.1.

Supports all countries globally, all admin levels (0-5).
Formats: GeoPackage (all levels in one file) or GeoJSON (per level).
"""

import logging
import time
from pathlib import Path

import httpx

from .base import BaseScraper, ExtractResult
from ..regions import get_admin_levels

log = logging.getLogger("geodata_scraper.gadm")

GADM_VERSION = "4.1"
GADM_PREFIX = "gadm41"
BASE_URL = f"https://geodata.ucdavis.edu/gadm/gadm{GADM_VERSION}"

# URL patterns
URLS = {
    "gpkg": f"{BASE_URL}/gpkg/{GADM_PREFIX}_{{iso3}}.gpkg",
    "shp": f"{BASE_URL}/shp/{GADM_PREFIX}_{{iso3}}_shp.zip",
    "geojson": f"{BASE_URL}/json/{GADM_PREFIX}_{{iso3}}_{{level}}.json",
}


class GADMScraper(BaseScraper):
    name = "gadm"
    description = "GADM v4.1 — Global Administrative Areas (admin boundaries level 0-5)"

    def __init__(self, format: str = "gpkg"):
        self.format = format

    def build_tasks(self, countries: list[str], **kwargs) -> list[dict]:
        storage_dir = kwargs.get("storage_dir", Path("/tmp/geodata_scraper"))
        tasks = []

        for iso3 in countries:
            iso3 = iso3.upper()
            max_level = get_admin_levels(iso3)

            if self.format == "geojson":
                # One file per admin level
                for level in range(max_level + 1):
                    url = URLS["geojson"].format(iso3=iso3, level=level)
                    dest = storage_dir / "gadm" / iso3 / f"{GADM_PREFIX}_{iso3}_{level}.json"
                    tasks.append({
                        "url": url,
                        "dest": dest,
                        "iso3": iso3,
                        "admin_level": level,
                        "format": "geojson",
                    })
            else:
                # GeoPackage — single file, all levels
                url = URLS[self.format].format(iso3=iso3)
                ext = "gpkg" if self.format == "gpkg" else "shp.zip"
                dest = storage_dir / "gadm" / iso3 / f"{GADM_PREFIX}_{iso3}.{ext}"
                tasks.append({
                    "url": url,
                    "dest": dest,
                    "iso3": iso3,
                    "admin_level": -1,  # -1 = all levels in one file
                    "format": self.format,
                })

        return tasks

    def extract(self, task: dict, storage_dir: Path) -> ExtractResult:
        url = task["url"]
        dest = task["dest"]
        iso3 = task["iso3"]
        admin_level = task["admin_level"]
        fmt = task["format"]

        dest.parent.mkdir(parents=True, exist_ok=True)

        # Skip if already downloaded
        if dest.exists() and dest.stat().st_size > 0:
            log.info(f"SKIP (exists): {dest.name}")
            return ExtractResult(
                url=url, local_path=dest, iso3=iso3, admin_level=admin_level,
                format=fmt, size=dest.stat().st_size, success=True,
            )

        retries = 3
        timeout = 300
        chunk_size = 256 * 1024
        try:
            from ..settings import scraper_settings
            retries = scraper_settings.DOWNLOAD_RETRIES
            timeout = scraper_settings.DOWNLOAD_TIMEOUT
            chunk_size = scraper_settings.CHUNK_SIZE
        except Exception:
            pass  # standalone mode

        for attempt in range(1, retries + 1):
            try:
                with httpx.stream(
                    "GET", url,
                    timeout=timeout,
                    headers={"User-Agent": "geodata-scraper/0.1"},
                    follow_redirects=True,
                ) as resp:
                    resp.raise_for_status()
                    total = int(resp.headers.get("content-length", 0))
                    tmp = dest.with_suffix(dest.suffix + ".part")

                    downloaded = 0
                    with open(tmp, "wb") as f:
                        for chunk in resp.iter_bytes(chunk_size=chunk_size):
                            f.write(chunk)
                            downloaded += len(chunk)

                    tmp.rename(dest)
                    level_str = f"admin{admin_level}" if admin_level >= 0 else "all-levels"
                    log.info(f"OK: {iso3} {level_str} — {dest.name} ({_fmt_size(downloaded)})")

                    # For gpkg: inspect and log contained layers
                    if fmt == "gpkg":
                        _log_gpkg_contents(dest, iso3)

                    return ExtractResult(
                        url=url, local_path=dest, iso3=iso3, admin_level=admin_level,
                        format=fmt, size=downloaded, success=True,
                    )

            except (httpx.HTTPError, OSError) as e:
                log.warning(f"RETRY {attempt}/{retries}: {dest.name} — {e}")
                time.sleep(2 * attempt)

        log.error(f"FAILED: {dest.name}")
        return ExtractResult(
            url=url, local_path=dest, iso3=iso3, admin_level=admin_level,
            format=fmt, size=0, success=False, error=f"Failed after {retries} attempts",
        )


def _log_gpkg_contents(gpkg_path: Path, iso3: str):
    """Inspect GeoPackage and log admin levels + feature counts."""
    import subprocess
    try:
        result = subprocess.run(
            ["ogrinfo", "-so", "-q", str(gpkg_path)],
            capture_output=True, text=True, timeout=30,
        )
        layers = []
        for line in result.stdout.strip().splitlines():
            parts = line.strip().split(":")
            if len(parts) >= 2:
                name = parts[1].strip().split("(")[0].strip()
                geom = parts[1].strip().split("(")[1].rstrip(")").strip() if "(" in parts[1] else ""
                if name:
                    layers.append((name, geom))

        if layers:
            log.info(f"  {iso3} GeoPackage contains {len(layers)} layers:")
            for name, geom in layers:
                # Get feature count
                count_result = subprocess.run(
                    ["ogrinfo", "-so", str(gpkg_path), name],
                    capture_output=True, text=True, timeout=30,
                )
                feat_count = "?"
                for cline in count_result.stdout.splitlines():
                    if "Feature Count" in cline:
                        feat_count = cline.split(":")[1].strip()
                        break
                import re
                level_match = re.search(r"(\d+)$", name)
                level = f"admin{level_match.group(1)}" if level_match else name
                log.info(f"    {level:10s} — {feat_count:>6s} features ({geom})")
    except Exception as e:
        log.debug(f"Could not inspect {gpkg_path}: {e}")


def _fmt_size(n: int) -> str:
    for u in ("B", "KB", "MB", "GB"):
        if abs(n) < 1024:
            return f"{n:.1f} {u}"
        n /= 1024
    return f"{n:.1f} TB"
