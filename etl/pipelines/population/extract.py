"""Extract population rasters for GHA — WorldPop + GEE-based.

WorldPop: 100m constrained population estimates (direct HTTP)
LandScan: Available via GEE (Oak Ridge National Lab)
GHS-POP: Global Human Settlement Population (Copernicus/JRC via GEE)

All clipped to GHA as COGs.
"""

import json
import time
import ee
import geemap
from etl.core.config import init_ee, raw_dir, processed_dir, sync_to_ssd, IGAD_COUNTRIES
from etl.core.utils import (
    load_gha, ensure_gha_geojson, download_file, download_parallel, gdal_clip_to_cog,
)

# WorldPop constrained 100m (2020) — direct download per country
WORLDPOP_BASE = "https://data.worldpop.org/GIS/Population/Global_2000_2020_Constrained/2020/maxar_v1"
ISO3_WORLDPOP = {
    "DJI": "DJI", "ERI": "ERI", "ETH": "ETH", "KEN": "KEN", "SOM": "SOM",
    "SSD": "SSD", "SDN": "SDN", "UGA": "UGA", "TZA": "TZA", "RWA": "RWA", "BDI": "BDI",
}


def extract_worldpop():
    """Download WorldPop 100m constrained population for each IGAD country."""
    tile_dir = raw_dir("population/worldpop")
    out_file = processed_dir("population") / "worldpop_100m_gha.tif"

    if out_file.exists() and out_file.stat().st_size > 1_000_000:
        print(f"WorldPop cached: {out_file.stat().st_size / 1e6:.0f} MB")
        return

    # Download per country
    items = []
    for iso3 in IGAD_COUNTRIES:
        iso_lower = iso3.lower()
        fname = f"{iso_lower}_ppp_2020_UNadj_constrained.tif"
        url = f"{WORLDPOP_BASE}/{iso3}/{fname}"
        items.append((url, tile_dir / fname))

    print(f"Downloading WorldPop for {len(items)} countries...")
    ok, fail = download_parallel(items, desc="WorldPop", max_workers=4)
    print(f"  {ok} ok, {fail} failed")

    # Mosaic + clip to GHA
    tiffs = sorted(str(t) for t in tile_dir.glob("*.tif") if t.stat().st_size > 1000)
    if tiffs:
        print("Mosaicing + clipping to GHA...")
        cutline = ensure_gha_geojson()
        gdal_clip_to_cog(tiffs, out_file, cutline=cutline)
        print(f"WorldPop COG: {out_file.stat().st_size / 1e6:.0f} MB")


def extract_ghspop_gee():
    """Extract GHS-POP (JRC) via GEE — 100m population grid."""
    out_file = raw_dir("population") / "ghspop_2020_gha.tif"
    if out_file.exists():
        print(f"GHS-POP cached: {out_file.name}")
        return

    init_ee()
    gdf = load_gha()
    geojson = json.loads(gdf.to_json())
    roi = ee.Geometry(geojson["features"][0]["geometry"])

    print("Exporting GHS-POP 2020 via GEE (100m)...")
    img = ee.Image("JRC/GHSL/P2023A/GHS_POP/2020").select("population_count").clip(roi)
    geemap.ee_export_image(img, filename=str(out_file), scale=100, region=roi, file_per_band=False)
    print(f"  saved {out_file.name}")


def extract_landscan_gee():
    """Extract LandScan via GEE — ~1km population grid."""
    out_file = raw_dir("population") / "landscan_2022_gha.tif"
    if out_file.exists():
        print(f"LandScan cached: {out_file.name}")
        return

    init_ee()
    gdf = load_gha()
    geojson = json.loads(gdf.to_json())
    roi = ee.Geometry(geojson["features"][0]["geometry"])

    print("Exporting LandScan 2022 via GEE (~1km)...")
    # LandScan on GEE (if available)
    try:
        img = ee.Image("projects/sat-io/open-datasets/LANDSCAN_GLOBAL/landscan-global-2022").clip(roi)
        geemap.ee_export_image(img, filename=str(out_file), scale=1000, region=roi, file_per_band=False)
        print(f"  saved {out_file.name}")
    except Exception as e:
        print(f"  LandScan GEE failed: {e}")
        print("  LandScan may require manual download from https://landscan.ornl.gov/")


def main():
    t_start = time.time()

    extract_worldpop()

    # GEE-dependent — skip if GEE key not configured
    try:
        extract_ghspop_gee()
    except Exception as e:
        print(f"GHS-POP skipped (GEE not available): {e}")

    try:
        extract_landscan_gee()
    except Exception as e:
        print(f"LandScan skipped (GEE not available): {e}")

    sync_to_ssd("raw/population")
    sync_to_ssd("processed/population")
    print(f"\nDone in {time.time() - t_start:.0f}s")


if __name__ == "__main__":
    main()
