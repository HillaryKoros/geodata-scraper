#!/usr/bin/env python3
"""
Test GADM scraper — extract GeoPackage for IGAD+ countries.
Runs standalone (no Django needed for extract test).
"""

import sys
import time
import logging
from pathlib import Path

# Add package to path
sys.path.insert(0, str(Path(__file__).parent))

from geodata_scraper.scrapers.gadm import GADMScraper
from geodata_scraper.regions import get_countries, get_admin_levels

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)

STORAGE = Path("/home/kipngenok/Work/Systems/IGAD-ICPAC/Data/GADM")
REGION = "igad_plus"


def main():
    countries = get_countries(REGION)

    print("=" * 60)
    print("  GADM Scraper Test — IGAD+ Countries")
    print("=" * 60)
    print(f"  Countries: {', '.join(countries)} ({len(countries)})")
    print()
    for iso3 in countries:
        levels = get_admin_levels(iso3)
        print(f"    {iso3} — admin levels 0-{levels}")
    print("=" * 60)

    scraper = GADMScraper(format="gpkg")
    tasks = scraper.build_tasks(countries, storage_dir=STORAGE)
    print(f"\n  Download tasks: {len(tasks)} GeoPackage files")
    print(f"  Storage: {STORAGE}\n")

    start = time.time()
    results = scraper.extract_all(countries, STORAGE, format="gpkg")
    elapsed = time.time() - start

    ok = [r for r in results if r.success]
    failed = [r for r in results if not r.success]
    total_bytes = sum(r.size for r in ok)

    print("\n" + "=" * 60)
    print("  RESULTS")
    print("=" * 60)

    for r in sorted(ok, key=lambda x: x.iso3):
        size_mb = r.size / (1024 * 1024)
        print(f"  OK  {r.iso3}  {r.local_path.name:30s}  {size_mb:8.1f} MB")

    for r in failed:
        print(f"  FAIL  {r.iso3}  {r.error}")

    print()
    print(f"  Downloaded : {len(ok)}/{len(results)} files")
    print(f"  Total size : {total_bytes / (1024*1024):.1f} MB")
    print(f"  Failed     : {len(failed)}")
    print(f"  Time       : {elapsed:.1f}s")
    print(f"  Location   : {STORAGE}")
    print("=" * 60)


if __name__ == "__main__":
    main()
