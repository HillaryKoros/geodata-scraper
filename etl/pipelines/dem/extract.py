"""Extract Copernicus DEM GLO-90 for GHA.

Downloads 1x1 degree tiles from AWS (no auth), mosaics + clips to GHA as COG.
Source: https://copernicus-dem-90m.s3.eu-central-1.amazonaws.com/

Uses gdalbuildvrt → gdalwarp with cutline for clipping.
"""

import subprocess
import time
from pathlib import Path
from etl.core.config import raw_dir, processed_dir, sync_to_ssd
from etl.core.utils import ensure_gha_geojson, download_parallel

COPERNICUS_BASE = "https://copernicus-dem-90m.s3.eu-central-1.amazonaws.com"
LON_MIN, LAT_MIN, LON_MAX, LAT_MAX = 21, -12, 52, 23


def tile_urls() -> list[tuple[str, str]]:
    """Generate tile URLs covering GHA."""
    tiles = []
    for lat in range(LAT_MIN, LAT_MAX):
        for lon in range(LON_MIN, LON_MAX):
            ns = "N" if lat >= 0 else "S"
            ew = "E" if lon >= 0 else "W"
            fname = f"Copernicus_DSM_COG_30_{ns}{abs(lat):02d}_00_{ew}{abs(lon):03d}_00_DEM.tif"
            folder = fname.replace(".tif", "")
            url = f"{COPERNICUS_BASE}/{folder}/{fname}"
            tiles.append((fname, url))
    return tiles


def main():
    t_start = time.time()
    tile_dir = raw_dir("dem/copernicus_90m")
    out_file = processed_dir("dem") / "dem_90m_gha.tif"

    if out_file.exists() and out_file.stat().st_size > 1_000_000:
        print(f"Cached: {out_file.name} ({out_file.stat().st_size / 1e6:.0f} MB)")
        return

    cutline = ensure_gha_geojson()
    tiles = tile_urls()
    print(f"{len(tiles)} DEM tiles to download")

    items = [(url, tile_dir / fname) for fname, url in tiles]
    ok, fail = download_parallel(items, desc="DEM tiles", max_workers=8)
    print(f"Downloaded: {ok} ok, {fail} not available")

    tiffs = sorted(str(t) for t in tile_dir.glob("*.tif") if t.stat().st_size > 1000)
    if not tiffs:
        print("ERROR: No valid DEM tiles")
        return

    print(f"\nMosaicing {len(tiffs)} tiles + clipping to GHA...")
    vrt_file = out_file.parent / "_tmp_dem.vrt"
    tmp_clip = out_file.parent / "_tmp_dem_clip.tif"

    try:
        # Build VRT from all tiles
        subprocess.run(
            ["gdalbuildvrt", str(vrt_file)] + tiffs,
            check=True, capture_output=True,
        )

        # Warp + clip — use dstnodata instead of dstalpha for single-band DEM
        result = subprocess.run([
            "gdalwarp", "-of", "GTiff",
            "-t_srs", "EPSG:4326",
            "-dstnodata", "-9999",
            "-co", "COMPRESS=DEFLATE",
            "-co", "BIGTIFF=YES",
            "-co", "NUM_THREADS=ALL_CPUS",
            "-multi",
            "-wo", "NUM_THREADS=ALL_CPUS",
            "-wm", "4096",
            "-overwrite",
            "-cutline", str(cutline),
            "-crop_to_cutline",
            str(vrt_file), str(tmp_clip),
        ], capture_output=True, text=True)
        if result.returncode != 0:
            raise RuntimeError(f"gdalwarp failed:\n{result.stderr}")

        # Convert to COG
        result = subprocess.run([
            "gdal_translate", "-of", "COG",
            "-co", "COMPRESS=DEFLATE",
            "-co", "NUM_THREADS=ALL_CPUS",
            "-co", "OVERVIEWS=NONE",
            "-co", "BIGTIFF=YES",
            str(tmp_clip), str(out_file),
        ], capture_output=True, text=True)
        if result.returncode != 0:
            raise RuntimeError(f"gdal_translate failed:\n{result.stderr}")

        print(f"DEM COG: {out_file.stat().st_size / 1e6:.0f} MB")
    finally:
        for f in [vrt_file, tmp_clip]:
            if f.exists():
                f.unlink()

    sync_to_ssd("processed/dem")
    print(f"\nDone in {time.time() - t_start:.0f}s")


if __name__ == "__main__":
    main()
