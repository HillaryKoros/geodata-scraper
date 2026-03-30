"""Extract JRC Global Surface Water layers via GEE, clipped to GHA.

Downloads occurrence, recurrence, seasonality, max_extent as COGs.
Source: JRC/GSW1_4/GlobalSurfaceWater
"""

import json
import time
import ee
import geemap
import geopandas as gpd
from tqdm import tqdm
from etl.core.config import init_ee, raw_dir, sync_to_ssd
from etl.core.utils import load_gha

DATASET = "JRC/GSW1_4/GlobalSurfaceWater"
BANDS = ["occurrence", "recurrence", "seasonality", "max_extent"]
SCALE = 30


def main():
    t_start = time.time()
    init_ee()

    gdf = load_gha()
    geojson = json.loads(gdf.to_json())
    roi = ee.Geometry(geojson["features"][0]["geometry"])
    print("GHA boundary loaded")

    out_dir = raw_dir("jrc_water")
    for band in tqdm(BANDS, desc="JRC Water", unit="band"):
        out_file = out_dir / f"jrc_gsw_{band}_gha.tif"
        if out_file.exists():
            print(f"  skip {band} (exists)")
            continue

        img = ee.Image(DATASET).select(band).clip(roi)
        print(f"  exporting {band} ({SCALE}m)...")
        geemap.ee_export_image(img, filename=str(out_file), scale=SCALE, region=roi, file_per_band=False)

    sync_to_ssd("raw/jrc_water")
    print(f"\nDone in {time.time() - t_start:.0f}s")


if __name__ == "__main__":
    main()
