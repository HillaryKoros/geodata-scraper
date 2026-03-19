"""
HydroSHEDS scraper — downloads HydroBASINS and HydroRIVERS data.

HydroBASINS: watershed boundaries at Pfafstetter levels 1-12
HydroRIVERS: river network with Strahler order and discharge

Source: https://data.hydrosheds.org/
"""

import logging
import time
import zipfile
from pathlib import Path

import httpx

from .base import BaseScraper, ExtractResult

log = logging.getLogger("geodata_scraper.hydrosheds")

BASE_URL = "https://data.hydrosheds.org/file"

# Africa region files
HYDROBASINS_URLS = {
    "lev01": f"{BASE_URL}/HydroBASINS/standard/hybas_af_lev01_v1c.zip",
    "lev02": f"{BASE_URL}/HydroBASINS/standard/hybas_af_lev02_v1c.zip",
    "lev03": f"{BASE_URL}/HydroBASINS/standard/hybas_af_lev03_v1c.zip",
    "lev04": f"{BASE_URL}/HydroBASINS/standard/hybas_af_lev04_v1c.zip",
    "lev05": f"{BASE_URL}/HydroBASINS/standard/hybas_af_lev05_v1c.zip",
    "lev06": f"{BASE_URL}/HydroBASINS/standard/hybas_af_lev06_v1c.zip",
    "lev07": f"{BASE_URL}/HydroBASINS/standard/hybas_af_lev07_v1c.zip",
    "lev08": f"{BASE_URL}/HydroBASINS/standard/hybas_af_lev08_v1c.zip",
    "lev09": f"{BASE_URL}/HydroBASINS/standard/hybas_af_lev09_v1c.zip",
    "lev10": f"{BASE_URL}/HydroBASINS/standard/hybas_af_lev10_v1c.zip",
    "lev11": f"{BASE_URL}/HydroBASINS/standard/hybas_af_lev11_v1c.zip",
    "lev12": f"{BASE_URL}/HydroBASINS/standard/hybas_af_lev12_v1c.zip",
}

HYDRORIVERS_URL = f"{BASE_URL}/HydroRIVERS/HydroRIVERS_v10_af_shp.zip"

# Continent codes for global downloads
CONTINENTS = {
    "africa": "af",
    "asia": "as",
    "europe": "eu",
    "north_america": "na",
    "south_america": "sa",
    "oceania": "au",
    "arctic": "ar",
    "greenland": "gr",
    "siberia": "si",
}


class HydroSHEDSScraper(BaseScraper):
    name = "hydrosheds"
    description = (
        "HydroSHEDS — HydroBASINS (watershed boundaries) + HydroRIVERS (river network)"
    )

    def __init__(
        self,
        continent: str = "africa",
        levels: list[int] | None = None,
        include_rivers: bool = True,
    ):
        self.continent = continent
        self.cont_code = CONTINENTS.get(continent, "af")
        self.levels = levels or list(range(1, 13))  # default: all 12 Pfafstetter levels
        self.include_rivers = include_rivers

    def build_tasks(self, countries: list[str] = None, **kwargs) -> list[dict]:
        storage_dir = kwargs.get("storage_dir", Path("/tmp/geodata_scraper"))
        tasks = []

        # HydroBASINS — one file per level (continent-wide)
        for level in self.levels:
            level_str = f"{level:02d}"
            url = f"{BASE_URL}/HydroBASINS/standard/hybas_{self.cont_code}_lev{level_str}_v1c.zip"
            dest = (
                storage_dir
                / "hydrosheds"
                / f"hybas_{self.cont_code}_lev{level_str}_v1c.zip"
            )
            tasks.append(
                {
                    "url": url,
                    "dest": dest,
                    "iso3": self.cont_code.upper(),
                    "admin_level": level,
                    "format": "shp_zip",
                    "layer_type": "basins",
                }
            )

        # HydroRIVERS
        if self.include_rivers:
            url = f"{BASE_URL}/HydroRIVERS/HydroRIVERS_v10_{self.cont_code}_shp.zip"
            dest = (
                storage_dir / "hydrosheds" / f"HydroRIVERS_v10_{self.cont_code}_shp.zip"
            )
            tasks.append(
                {
                    "url": url,
                    "dest": dest,
                    "iso3": self.cont_code.upper(),
                    "admin_level": -1,
                    "format": "shp_zip",
                    "layer_type": "rivers",
                }
            )

        return tasks

    def extract(self, task: dict, storage_dir: Path) -> ExtractResult:
        url = task["url"]
        dest = task["dest"]
        iso3 = task["iso3"]
        admin_level = task["admin_level"]
        layer_type = task.get("layer_type", "basins")

        dest.parent.mkdir(parents=True, exist_ok=True)

        # Check if already extracted
        extract_dir = dest.with_suffix("")
        if dest.exists() and dest.stat().st_size > 0:
            log.info(f"SKIP (exists): {dest.name}")
            # Extract zip if not already done
            if not extract_dir.exists():
                self._extract_zip(dest, extract_dir)
            return ExtractResult(
                url=url,
                local_path=extract_dir,
                iso3=iso3,
                admin_level=admin_level,
                format="shp",
                size=dest.stat().st_size,
                success=True,
            )

        retries = 3
        timeout = 600  # HydroSHEDS files can be large
        chunk_size = 256 * 1024
        try:
            from ..settings import scraper_settings

            retries = scraper_settings.DOWNLOAD_RETRIES
            timeout = max(scraper_settings.DOWNLOAD_TIMEOUT, 600)
            chunk_size = scraper_settings.CHUNK_SIZE
        except Exception:
            pass

        for attempt in range(1, retries + 1):
            try:
                with httpx.stream(
                    "GET",
                    url,
                    timeout=timeout,
                    headers={"User-Agent": "geodata-scraper/0.1"},
                    follow_redirects=True,
                ) as resp:
                    resp.raise_for_status()
                    tmp = dest.with_suffix(dest.suffix + ".part")
                    downloaded = 0

                    with open(tmp, "wb") as f:
                        for chunk in resp.iter_bytes(chunk_size=chunk_size):
                            f.write(chunk)
                            downloaded += len(chunk)

                    tmp.rename(dest)

                    level_str = (
                        f"level {admin_level:02d}" if admin_level >= 0 else "rivers"
                    )
                    log.info(
                        f"OK: HydroSHEDS {layer_type} {level_str} — {dest.name} ({_fmt_size(downloaded)})"
                    )

                    # Extract zip
                    self._extract_zip(dest, extract_dir)

                    # Log contents
                    shp_files = list(extract_dir.rglob("*.shp"))
                    for shp in shp_files:
                        log.info(f"    {shp.name}")

                    return ExtractResult(
                        url=url,
                        local_path=extract_dir,
                        iso3=iso3,
                        admin_level=admin_level,
                        format="shp",
                        size=downloaded,
                        success=True,
                    )

            except (httpx.HTTPError, OSError) as e:
                log.warning(f"RETRY {attempt}/{retries}: {dest.name} — {e}")
                time.sleep(2 * attempt)

        log.error(f"FAILED: {dest.name}")
        return ExtractResult(
            url=url,
            local_path=dest,
            iso3=iso3,
            admin_level=admin_level,
            format="shp",
            size=0,
            success=False,
            error=f"Failed after {retries} attempts",
        )

    def _extract_zip(self, zip_path: Path, extract_dir: Path):
        """Extract shapefile zip."""
        try:
            extract_dir.mkdir(parents=True, exist_ok=True)
            with zipfile.ZipFile(zip_path, "r") as zf:
                zf.extractall(extract_dir)
            log.info(f"  Extracted: {zip_path.name} → {extract_dir.name}/")
        except zipfile.BadZipFile as e:
            log.error(f"  Bad zip: {zip_path.name} — {e}")


def _fmt_size(n: int) -> str:
    for u in ("B", "KB", "MB", "GB"):
        if abs(n) < 1024:
            return f"{n:.1f} {u}"
        n /= 1024
    return f"{n:.1f} TB"
