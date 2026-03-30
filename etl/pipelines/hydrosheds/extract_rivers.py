"""Extract HydroSHEDS HydroRIVERS — clipped to GHA.

Downloads Africa shapefile from https://data.hydrosheds.org/
Clips to GHA dissolved boundary.
"""

import time
import urllib.request
import zipfile
import geopandas as gpd
from etl.core.config import raw_dir, processed_dir, sync_to_ssd
from etl.core.utils import load_gha_geom

URL = "https://data.hydrosheds.org/file/HydroRIVERS/HydroRIVERS_v10_af_shp.zip"


def download_hydrosheds(url: str, out_file, extract_dir):
    """Download with browser-like headers (HydroSHEDS blocks default urllib agent)."""
    if extract_dir.exists() and any(extract_dir.rglob("*.shp")):
        print("Using cached HydroRIVERS data")
        return
    if not out_file.exists() or out_file.stat().st_size < 1000:
        print(f"Downloading {url}...")
        req = urllib.request.Request(url, headers={
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) spatial-db/1.0",
        })
        with urllib.request.urlopen(req, timeout=600) as resp:
            out_file.write_bytes(resp.read())
        print(f"  {out_file.stat().st_size / 1e6:.0f} MB")
    extract_dir.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(out_file) as zf:
        zf.extractall(extract_dir)


def main():
    t_start = time.time()
    gha_geom = load_gha_geom()
    print("GHA boundary loaded")

    out_dir = raw_dir("hydrorivers")
    zip_path = out_dir / "HydroRIVERS_v10_af_shp.zip"
    extract_dir = out_dir / "extracted"

    download_hydrosheds(URL, zip_path, extract_dir)

    shp_files = list(extract_dir.rglob("*.shp"))
    if not shp_files:
        raise FileNotFoundError("No .shp found in zip")

    print(f"Reading {shp_files[0].name}...")
    gdf = gpd.read_file(shp_files[0])
    print(f"  {len(gdf)} rivers in Africa")

    print("Clipping to GHA...")
    gdf_gha = gpd.clip(gdf, gha_geom)
    print(f"  {len(gdf_gha)} river segments in GHA")

    out_file = processed_dir("hydrorivers") / "hydrorivers_gha.parquet"
    gdf_gha.to_parquet(out_file)
    print(f"Saved: {out_file.name} ({out_file.stat().st_size / 1e6:.1f} MB)")

    sync_to_ssd("processed/hydrorivers")
    print(f"\nDone in {time.time() - t_start:.0f}s")


if __name__ == "__main__":
    main()
