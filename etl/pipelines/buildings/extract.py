"""Extract building footprints for GHA — Microsoft + Google Open Buildings.

Microsoft: https://minedbuildings.blob.core.windows.net/global-buildings/
Google: https://sites.research.google/open-buildings/

Downloads country-level GeoJSON/CSV, filters to GHA.
Both are open-source alternatives to Facebook/Meta buildings.
"""

import csv
import json
import time
import urllib.request
import pandas as pd
import geopandas as gpd
from io import StringIO
from pathlib import Path
from etl.core.config import raw_dir, processed_dir, sync_to_ssd, IGAD_COUNTRIES, ISO3_TO_NAME
from etl.core.utils import load_gha_geom, download_file

# Microsoft open buildings — country-level links
MS_BUILDINGS_INDEX = "https://minedbuildings.blob.core.windows.net/global-buildings/dataset-links.csv"

# Country name mapping for Microsoft dataset (uses different naming)
ISO2_MAP = {
    "DJI": "DJ", "ERI": "ER", "ETH": "ET", "KEN": "KE", "SOM": "SO",
    "SSD": "SS", "SDN": "SD", "UGA": "UG", "TZA": "TZ", "RWA": "RW", "BDI": "BI",
}


def get_ms_building_urls() -> dict[str, list[str]]:
    """Parse Microsoft buildings index CSV to get URLs per country."""
    print("Fetching Microsoft buildings index...")
    resp = urllib.request.urlopen(MS_BUILDINGS_INDEX)
    text = resp.read().decode()
    reader = csv.DictReader(StringIO(text))

    # Map ISO2 codes we care about
    iso2_set = set(ISO2_MAP.values())
    urls_by_country = {}

    for row in reader:
        loc = row.get("Location", "")
        # Location format varies — check against our countries
        for iso3, iso2 in ISO2_MAP.items():
            if iso2 in loc or ISO3_TO_NAME.get(iso3, "") in loc:
                urls_by_country.setdefault(iso3, []).append(row.get("Url", row.get("QuadKey", "")))

    return urls_by_country


def main():
    t_start = time.time()
    gha_geom = load_gha_geom()
    print("GHA boundary loaded")

    out_file = processed_dir("buildings") / "ms_buildings_gha_count.parquet"
    if out_file.exists() and out_file.stat().st_size > 1000:
        print(f"Cached: {out_file.name}")
        return

    # Download Microsoft buildings index
    try:
        urls = get_ms_building_urls()
        print(f"Found building data for {len(urls)} countries")

        all_gdfs = []
        for iso3, country_urls in urls.items():
            country_dir = raw_dir(f"buildings/ms/{iso3}")
            print(f"\n{ISO3_TO_NAME.get(iso3, iso3)}: {len(country_urls)} files")

            for i, url in enumerate(country_urls[:5]):  # Limit to first 5 per country
                out_path = country_dir / f"buildings_{iso3}_{i}.geojsonl"
                if download_file(url, out_path):
                    try:
                        gdf = gpd.read_file(out_path)
                        all_gdfs.append(gdf)
                        print(f"  {len(gdf)} buildings")
                    except Exception as e:
                        print(f"  parse error: {e}")

        if all_gdfs:
            combined = gpd.GeoDataFrame(pd.concat(all_gdfs, ignore_index=True))
            combined.to_parquet(out_file)
            print(f"\nTotal: {len(combined)} buildings")

    except Exception as e:
        print(f"Microsoft buildings failed: {e}")
        print("Buildings extraction requires manual download — dataset is very large")
        print("Consider using Google Earth Engine: ee.FeatureCollection('GOOGLE/Research/open-buildings/v3/polygons')")

    sync_to_ssd("processed/buildings")
    print(f"\nDone in {time.time() - t_start:.0f}s")


if __name__ == "__main__":
    main()
