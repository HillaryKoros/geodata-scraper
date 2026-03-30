"""Extract JRC Global Flood Hazard Maps — direct HTTP from JRC FTP.

Downloads tiled GeoTIFFs covering GHA region, filters by bbox.
Source: https://jeodpp.jrc.ec.europa.eu/ftp/jrc-opendata/CEMS-GLOFAS/flood_hazard/
"""

import re
import time
import urllib.request
from etl.core.config import raw_dir, sync_to_ssd
from etl.core.utils import download_file, download_parallel, tiles_in_bbox

BASE_URL = "https://jeodpp.jrc.ec.europa.eu/ftp/jrc-opendata/CEMS-GLOFAS/flood_hazard"
RETURN_PERIODS = [10, 20, 50, 75, 100]
GHA_BBOX = (20, -20, 52, 23)  # lon_min, lat_min, lon_max, lat_max


def list_gha_tiles(rp: int) -> list[tuple[str, str]]:
    """Fetch directory listing, return (filename, url) for tiles in GHA."""
    url = f"{BASE_URL}/RP{rp}/"
    html = urllib.request.urlopen(url).read().decode()
    files = re.findall(r'href="([^"]*_depth\.tif)"', html)
    gha_files = tiles_in_bbox(files, GHA_BBOX)
    return [(f, f"{url}{f}") for f in gha_files]


def main():
    t_start = time.time()

    for rp in RETURN_PERIODS:
        rp_dir = raw_dir(f"jrc_flood/RP{rp}")

        print(f"\nRP{rp}: scanning server...")
        tiles = list_gha_tiles(rp)
        print(f"  {len(tiles)} tiles for GHA")

        items = [(url, rp_dir / fname) for fname, url in tiles]
        ok, fail = download_parallel(items, desc=f"RP{rp}", max_workers=4)
        print(f"  {ok} ok, {fail} failed")

    # Permanent water bodies
    print("\nPermanent water bodies...")
    wb_dir = raw_dir("jrc_flood/water_bodies")
    try:
        url = f"{BASE_URL}/Permanent_WaterBodies/"
        html = urllib.request.urlopen(url).read().decode()
        files = re.findall(r'href="([^"]*_depth\.tif)"', html)
        gha_files = tiles_in_bbox(files, GHA_BBOX)
        items = [(f"{url}{f}", wb_dir / f) for f in gha_files]
        ok, _ = download_parallel(items, desc="WaterBodies", max_workers=4)
        print(f"  {ok} water body tiles")
    except Exception:
        print("  skipped (not available)")

    sync_to_ssd("raw/jrc_flood")

    total_files = sum(1 for _ in raw_dir("jrc_flood").rglob("*.tif"))
    total_mb = sum(f.stat().st_size for f in raw_dir("jrc_flood").rglob("*.tif")) / 1e6
    print(f"\nDone in {time.time() - t_start:.0f}s — {total_files} files, {total_mb:.0f} MB")


if __name__ == "__main__":
    main()
