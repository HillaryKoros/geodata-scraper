"""
Generic HTTP scraper — download any geodata from HTTPS URLs.

Usage:
    source = DataSource(
        source_type="generic",
        protocol="https",
        base_url="https://example.com/data/{filename}",
    )
"""

import logging
import time
from pathlib import Path

import httpx

from .base import BaseScraper, ExtractResult

log = logging.getLogger("geodata_scraper.http")


class HTTPScraper(BaseScraper):
    name = "http"
    description = "Generic HTTPS geodata scraper"

    def __init__(self, urls: list[str] | None = None, headers: dict | None = None):
        self.urls = urls or []
        self.headers = headers or {"User-Agent": "geodata-scraper/0.1"}

    def build_tasks(self, countries: list[str] = None, **kwargs) -> list[dict]:
        storage_dir = kwargs.get("storage_dir", Path("/tmp/geodata_scraper"))
        tasks = []

        for url in self.urls:
            filename = url.split("/")[-1].split("?")[0]
            dest = storage_dir / "http" / filename
            fmt = Path(filename).suffix.lstrip(".")
            tasks.append(
                {
                    "url": url,
                    "dest": dest,
                    "iso3": "",
                    "admin_level": -1,
                    "format": fmt,
                }
            )

        return tasks

    def extract(self, task: dict, storage_dir: Path) -> ExtractResult:
        url = task["url"]
        dest = task["dest"]
        dest.parent.mkdir(parents=True, exist_ok=True)

        if dest.exists() and dest.stat().st_size > 0:
            return ExtractResult(
                url=url,
                local_path=dest,
                iso3=task["iso3"],
                admin_level=task["admin_level"],
                format=task["format"],
                size=dest.stat().st_size,
                success=True,
            )

        from ..settings import scraper_settings

        for attempt in range(1, scraper_settings.DOWNLOAD_RETRIES + 1):
            try:
                with httpx.stream(
                    "GET",
                    url,
                    timeout=scraper_settings.DOWNLOAD_TIMEOUT,
                    headers=self.headers,
                    follow_redirects=True,
                ) as resp:
                    resp.raise_for_status()
                    tmp = dest.with_suffix(dest.suffix + ".part")
                    downloaded = 0

                    with open(tmp, "wb") as f:
                        for chunk in resp.iter_bytes(
                            chunk_size=scraper_settings.CHUNK_SIZE
                        ):
                            f.write(chunk)
                            downloaded += len(chunk)

                    tmp.rename(dest)
                    log.info(f"OK: {dest.name} ({downloaded} bytes)")
                    return ExtractResult(
                        url=url,
                        local_path=dest,
                        iso3=task["iso3"],
                        admin_level=task["admin_level"],
                        format=task["format"],
                        size=downloaded,
                        success=True,
                    )

            except (httpx.HTTPError, OSError) as e:
                log.warning(f"RETRY {attempt}: {dest.name} — {e}")
                time.sleep(2 * attempt)

        return ExtractResult(
            url=url,
            local_path=dest,
            iso3=task["iso3"],
            admin_level=task["admin_level"],
            format=task["format"],
            size=0,
            success=False,
            error="Download failed",
        )
