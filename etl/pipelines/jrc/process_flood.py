"""Mosaic and clip JRC flood tiles to GHA boundary.

For each return period: merge GHA tiles via rasterio → mask to boundary → save.
Pure Python — no GDAL CLI dependency.
"""

import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

import geopandas as gpd
import numpy as np
import rasterio
from rasterio.merge import merge
from rasterio.mask import mask
from tqdm import tqdm

from etl.core.config import raw_dir, processed_dir, sync_to_ssd
from etl.core.utils import load_gha_geom, tiles_in_bbox

RETURN_PERIODS = [10, 20, 50, 75, 100]
GHA_BBOX = (21.0, -12.0, 52.0, 24.0)


def process_rp(rp: int) -> tuple[int, str, float]:
    """Merge + clip one return period to GHA. Returns (rp, status, size_mb)."""
    out_dir = processed_dir("jrc_flood")
    out_file = out_dir / f"jrc_flood_rp{rp}_gha.tif"

    if out_file.exists() and out_file.stat().st_size > 1_000_000:
        return (rp, "cached", out_file.stat().st_size / 1e6)

    tile_dir = raw_dir(f"jrc_flood/RP{rp}")
    all_tifs = sorted(tile_dir.glob("*.tif"))

    # Filter to only tiles that intersect GHA
    gha_names = tiles_in_bbox([t.name for t in all_tifs], GHA_BBOX)
    tiffs = [tile_dir / n for n in gha_names if (tile_dir / n).stat().st_size > 1000]

    if not tiffs:
        return (rp, "no tiles", 0)

    try:
        gha_geom = load_gha_geom()

        # Merge directly with bounds from GHA to limit memory — only read
        # the portion of each tile that overlaps our AOI
        datasets = [rasterio.open(t) for t in tiffs]
        mosaic, mosaic_transform = merge(
            datasets,
            nodata=-9999.0,
            bounds=gha_geom.bounds,  # crop to GHA extent during merge
        )
        profile = datasets[0].profile.copy()
        for ds in datasets:
            ds.close()

        profile.update(
            driver="GTiff",
            height=mosaic.shape[1],
            width=mosaic.shape[2],
            transform=mosaic_transform,
            compress="deflate",
            nodata=-9999.0,
            tiled=True,
            blockxsize=256,
            blockysize=256,
        )

        # Write merged raster cropped to GHA bbox, then mask to exact boundary
        tmp_file = out_dir / f"_tmp_rp{rp}_merged.tif"
        with rasterio.open(tmp_file, "w", **profile) as dst:
            dst.write(mosaic)
        del mosaic  # free memory

        # Mask to exact GHA boundary (Tanzania + Zanzibar included)
        with rasterio.open(tmp_file) as src:
            clipped, clipped_transform = mask(
                src, [gha_geom], crop=True, nodata=-9999.0
            )
            clip_profile = src.profile.copy()

        clip_profile.update(
            height=clipped.shape[1],
            width=clipped.shape[2],
            transform=clipped_transform,
            compress="deflate",
            tiled=True,
            blockxsize=256,
            blockysize=256,
        )

        with rasterio.open(out_file, "w", **clip_profile) as dst:
            dst.write(clipped)
        del clipped  # free memory

        tmp_file.unlink(missing_ok=True)
        return (rp, "ok", out_file.stat().st_size / 1e6)

    except Exception as e:
        out_file.unlink(missing_ok=True)
        return (rp, f"FAILED: {e}", 0)


def main():
    t_start = time.time()
    processed_dir("jrc_flood")

    print(f"Processing {len(RETURN_PERIODS)} return periods (2 workers)...\n")

    with ProcessPoolExecutor(max_workers=2) as pool:
        futures = {pool.submit(process_rp, rp): rp for rp in RETURN_PERIODS}
        with tqdm(total=len(futures), desc="JRC Flood", unit="rp") as pbar:
            for fut in as_completed(futures):
                rp, status, size_mb = fut.result()
                pbar.set_postfix_str(f"RP{rp}: {status} ({size_mb:.1f}MB)")
                pbar.update(1)

    print("\nResults:")
    total_mb = 0
    for rp in RETURN_PERIODS:
        f = processed_dir("jrc_flood") / f"jrc_flood_rp{rp}_gha.tif"
        if f.exists():
            mb = f.stat().st_size / 1e6
            total_mb += mb
            print(f"  RP{rp}: {mb:.1f} MB")
        else:
            print(f"  RP{rp}: MISSING")

    sync_to_ssd("processed/jrc_flood")
    print(f"\nDone in {time.time() - t_start:.0f}s — {total_mb:.0f} MB total")


if __name__ == "__main__":
    main()
