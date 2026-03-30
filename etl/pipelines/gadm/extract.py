"""Extract GADM admin boundaries for all 11 IGAD countries via GEE.

Uses geemap + Earth Engine to pull FAO GAUL (server-side filtering).
All admin levels → GeoParquet → PostGIS + DuckDB → SSD sync.
"""

import time
import ee
import geemap
import geopandas as gpd
from tqdm import tqdm
from etl.core.config import init_ee, ISO3_TO_NAME, raw_dir, sync_to_ssd
from etl.core.load import load_parquets
from etl.core.utils import fix_geometry

GAUL_ASSETS = {
    0: "FAO/GAUL/2015/level0",
    1: "FAO/GAUL/2015/level1",
    2: "FAO/GAUL/2015/level2",
}


def extract_level(level: int) -> str:
    """Extract one admin level for all IGAD countries from GEE."""
    out_dir = raw_dir("gadm")
    out_file = out_dir / f"igad_adm{level}.parquet"

    if out_file.exists():
        print(f"  skip adm{level} (exists)")
        return out_file

    country_names = list(ISO3_TO_NAME.values())
    fc = ee.FeatureCollection(GAUL_ASSETS[level])
    filtered = fc.filter(ee.Filter.inList("ADM0_NAME", country_names))

    tmp_geojson = out_dir / f"igad_adm{level}.geojson"
    print(f"  downloading adm{level} from GEE...")
    geemap.ee_to_geojson(filtered, filename=str(tmp_geojson))

    gdf = gpd.read_file(tmp_geojson)
    gdf["geometry"] = gdf["geometry"].apply(fix_geometry)
    gdf.to_parquet(out_file)
    tmp_geojson.unlink(missing_ok=True)
    print(f"  adm{level}: {len(gdf)} features")
    return out_file


def split_per_country(level: int):
    """Save per-country files for convenience."""
    out_dir = raw_dir("gadm")
    merged = out_dir / f"igad_adm{level}.parquet"
    if not merged.exists():
        return

    gdf = gpd.read_parquet(merged)
    if "ADM0_NAME" not in gdf.columns:
        return

    for iso3, name in ISO3_TO_NAME.items():
        subset = gdf[gdf["ADM0_NAME"] == name]
        if len(subset) > 0:
            subset.to_parquet(out_dir / f"gadm_{iso3}_adm{level}.parquet")


def main():
    t_start = time.time()
    init_ee()

    files = []
    for level in tqdm([0, 1, 2], desc="GADM levels", unit="level"):
        result = extract_level(level)
        if result:
            files.append(result)
            split_per_country(level)

    load_parquets(files)

    print("\nSync to SSD...")
    sync_to_ssd("raw/gadm")

    print(f"\nDone in {time.time() - t_start:.0f}s")


if __name__ == "__main__":
    main()
