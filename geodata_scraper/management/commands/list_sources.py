"""
List available scraper backends.

Usage:
    python manage.py list_sources
"""

from django.core.management.base import BaseCommand
from geodata_scraper.scrapers import ScraperRegistry
from geodata_scraper.regions import REGIONS


class Command(BaseCommand):
    help = "List available scraper backends and regions"

    def handle(self, *args, **options):
        self.stdout.write("\n=== Scraper Backends ===\n")
        for name, desc in ScraperRegistry.list().items():
            self.stdout.write(f"  {name:12s} — {desc}")

        self.stdout.write("\n=== Region Presets ===\n")
        for name, codes in REGIONS.items():
            self.stdout.write(
                f"  {name:12s} — {len(codes)} countries: {', '.join(codes[:5])}{'...' if len(codes) > 5 else ''}"
            )

        self.stdout.write("")
