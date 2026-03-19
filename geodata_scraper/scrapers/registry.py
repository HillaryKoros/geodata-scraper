"""
Scraper registry — auto-discover and register scraper backends.
"""

from .base import BaseScraper

_REGISTRY: dict[str, type[BaseScraper]] = {}


def register(scraper_class: type[BaseScraper]):
    """Register a scraper class."""
    _REGISTRY[scraper_class.name] = scraper_class
    return scraper_class


def get_scraper(name: str, **kwargs) -> BaseScraper:
    """Get a scraper instance by name."""
    if name not in _REGISTRY:
        raise ValueError(f"Unknown scraper: {name}. Available: {list(_REGISTRY.keys())}")
    return _REGISTRY[name](**kwargs)


def list_scrapers() -> dict[str, str]:
    """Return {name: description} for all registered scrapers."""
    return {name: cls.description for name, cls in _REGISTRY.items()}


class ScraperRegistry:
    """Namespace for registry functions."""
    register = staticmethod(register)
    get = staticmethod(get_scraper)
    list = staticmethod(list_scrapers)


# Auto-register built-in scrapers
from .gadm import GADMScraper  # noqa: E402
from .http import HTTPScraper  # noqa: E402
from .api import APIScraper  # noqa: E402
from .ftp import FTPScraper  # noqa: E402
from .hydrosheds import HydroSHEDSScraper  # noqa: E402

register(GADMScraper)
register(HTTPScraper)
register(APIScraper)
register(FTPScraper)
register(HydroSHEDSScraper)
