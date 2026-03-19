"""
Base scraper protocol — all scrapers implement this interface.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path


@dataclass
class ExtractResult:
    """Result of extracting one file."""

    url: str
    local_path: Path
    iso3: str
    admin_level: int
    format: str
    size: int
    success: bool
    error: str = ""


class BaseScraper(ABC):
    """
    Base class for all geodata scrapers.

    Subclass this to add new data sources.
    Implement extract() to download files from your source.
    """

    name: str = "base"
    description: str = "Base scraper"

    @abstractmethod
    def build_tasks(self, countries: list[str], **kwargs) -> list[dict]:
        """
        Build a list of download tasks.

        Returns list of dicts:
            {"url": str, "dest": Path, "iso3": str, "admin_level": int, "format": str}
        """
        ...

    @abstractmethod
    def extract(self, task: dict, storage_dir: Path) -> ExtractResult:
        """
        Download a single file from the source.
        """
        ...

    def extract_all(
        self, countries: list[str], storage_dir: Path, workers: int = 3, **kwargs
    ) -> list[ExtractResult]:
        """Download all files for given countries. Override for async/parallel."""
        import concurrent.futures

        try:
            from ..settings import scraper_settings

            workers = scraper_settings.DOWNLOAD_WORKERS
        except Exception:
            pass  # standalone mode — use default

        tasks = self.build_tasks(countries, storage_dir=storage_dir, **kwargs)
        results = []

        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {
                pool.submit(self.extract, task, storage_dir): task for task in tasks
            }
            for future in concurrent.futures.as_completed(futures):
                results.append(future.result())

        return results
