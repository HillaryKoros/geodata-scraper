"""
REST API scraper — fetch GeoJSON from any API endpoint.

Handles pagination, auth tokens, and GeoJSON/JSON responses.
"""

import json
import logging
from pathlib import Path

import httpx

from .base import BaseScraper, ExtractResult

log = logging.getLogger("geodata_scraper.api")


class APIScraper(BaseScraper):
    name = "api"
    description = "REST API GeoJSON scraper with pagination support"

    def __init__(
        self,
        endpoints: list[dict] | None = None,
        headers: dict | None = None,
        auth_token: str | None = None,
    ):
        """
        endpoints: list of {"url": str, "name": str, "params": dict}
        """
        self.endpoints = endpoints or []
        self.headers = headers or {"User-Agent": "geodata-scraper/0.1"}
        if auth_token:
            self.headers["Authorization"] = f"Bearer {auth_token}"

    def build_tasks(self, countries: list[str] = None, **kwargs) -> list[dict]:
        storage_dir = kwargs.get("storage_dir", Path("/tmp/geodata_scraper"))
        tasks = []

        for ep in self.endpoints:
            name = ep.get("name", "api_data")
            dest = storage_dir / "api" / f"{name}.geojson"
            tasks.append({
                "url": ep["url"],
                "dest": dest,
                "iso3": ep.get("iso3", ""),
                "admin_level": ep.get("admin_level", -1),
                "format": "geojson",
                "params": ep.get("params", {}),
            })

        return tasks

    def extract(self, task: dict, storage_dir: Path) -> ExtractResult:
        url = task["url"]
        dest = task["dest"]
        params = task.get("params", {})
        dest.parent.mkdir(parents=True, exist_ok=True)

        if dest.exists() and dest.stat().st_size > 0:
            return ExtractResult(
                url=url, local_path=dest, iso3=task["iso3"],
                admin_level=task["admin_level"], format="geojson",
                size=dest.stat().st_size, success=True,
            )

        from ..settings import scraper_settings

        try:
            all_features = []
            next_url = url
            page = 0

            while next_url:
                resp = httpx.get(
                    next_url,
                    params=params if page == 0 else {},
                    headers=self.headers,
                    timeout=scraper_settings.DOWNLOAD_TIMEOUT,
                    follow_redirects=True,
                )
                resp.raise_for_status()
                data = resp.json()

                # Handle GeoJSON FeatureCollection
                if "features" in data:
                    all_features.extend(data["features"])
                elif data.get("type") == "Feature":
                    all_features.append(data)

                # Pagination: check for 'next' link
                next_url = None
                if "links" in data:
                    for link in data["links"]:
                        if link.get("rel") == "next":
                            next_url = link["href"]
                elif "next" in data:
                    next_url = data["next"]

                page += 1
                params = {}  # Clear params after first page

            # Write collected features
            geojson = {
                "type": "FeatureCollection",
                "features": all_features,
            }
            with open(dest, "w") as f:
                json.dump(geojson, f)

            size = dest.stat().st_size
            log.info(f"OK: {dest.name} ({len(all_features)} features, {size} bytes)")

            return ExtractResult(
                url=url, local_path=dest, iso3=task["iso3"],
                admin_level=task["admin_level"], format="geojson",
                size=size, success=True,
            )

        except (httpx.HTTPError, json.JSONDecodeError, KeyError) as e:
            log.error(f"FAILED: {dest.name} — {e}")
            return ExtractResult(
                url=url, local_path=dest, iso3=task["iso3"],
                admin_level=task["admin_level"], format="geojson",
                size=0, success=False, error=str(e),
            )
