"""Extract HydroSHEDS HydroBASINS — all 12 levels for GHA.

Downloads from https://data.hydrosheds.org/
Filters basins that INTERSECT GHA (keeps full geometry, no clipping).
"""

import time
import zipfile
import geopandas as gpd
from tqdm import tqdm
from etl.core.config import raw_dir, processed_dir, sync_to_ssd
from etl.core.utils import load_gha_geom, download_file

BASE_URL = "https://data.hydrosheds.org/file/HydroBASINS/standard"
LEVELS = list(range(1, 13))


def download_level(level: int):
    """Download one HydroBASINS level for Africa."""
    fname = f"hybas_af_lev{level:02d}_v1c.zip"
    zip_path = raw_dir("hydrobasins") / fname
    url = f"{BASE_URL}/{fname}"
    download_file(url, zip_path)
    return zip_path


def extract_and_filter(level: int, gha_geom) -> tuple[int, int, str]:
    """Extract zip, filter basins intersecting GHA, save GeoParquet."""
    out_file = processed_dir("hydrobasins") / f"hydrobasins_lev{level:02d}_gha.parquet"
    if out_file.exists() and out_file.stat().st_size > 1000:
        return (level, len(gpd.read_parquet(out_file)), "cached")

    zip_dir = raw_dir("hydrobasins")
    zip_path = zip_dir / f"hybas_af_lev{level:02d}_v1c.zip"
    if not zip_path.exists():
        return (level, 0, "no zip")

    extract_dir = zip_dir / f"lev{level:02d}"
    with zipfile.ZipFile(zip_path) as zf:
        zf.extractall(extract_dir)

    shp_files = list(extract_dir.glob("*.shp"))
    if not shp_files:
        return (level, 0, "no shp")

    gdf = gpd.read_file(shp_files[0])
    gdf_gha = gdf[gdf.geometry.intersects(gha_geom)].copy()

    if len(gdf_gha) == 0:
        return (level, 0, "no intersection")

    gdf_gha.to_parquet(out_file)
    return (level, len(gdf_gha), "ok")


def main():
    t_start = time.time()
    gha_geom = load_gha_geom()
    print("GHA boundary loaded")

    print(f"\nDownloading {len(LEVELS)} levels...")
    for lev in tqdm(LEVELS, desc="Download", unit="lev"):
        download_level(lev)

    print(f"\nFiltering basins intersecting GHA...")
    results = []
    for lev in tqdm(LEVELS, desc="Filter", unit="lev"):
        results.append(extract_and_filter(lev, gha_geom))

    print("\nResults:")
    total = 0
    for lev, count, status in sorted(results):
        print(f"  Level {lev:2d}: {count:6d} basins ({status})")
        total += count
    print(f"  Total: {total} basins")

    sync_to_ssd("raw/hydrobasins")
    sync_to_ssd("processed/hydrobasins")
    print(f"\nDone in {time.time() - t_start:.0f}s")


if __name__ == "__main__":
    main()
