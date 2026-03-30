"""Mosaic and clip JRC flood tiles to GHA boundary as COGs.

For each return period: VRT mosaic → clip to GHA → COG.
Uses GDAL CLI with -dstalpha for correct nodata handling.
"""

import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from tqdm import tqdm
from etl.core.config import raw_dir, processed_dir, sync_to_ssd
from etl.core.utils import ensure_gha_geojson, gdal_clip_to_cog

RETURN_PERIODS = [10, 20, 50, 75, 100]


def process_rp(rp: int) -> tuple[int, str, float]:
    """Process one return period. Returns (rp, status, size_mb)."""
    out_file = processed_dir("jrc_flood") / f"jrc_flood_rp{rp}_gha.tif"

    if out_file.exists() and out_file.stat().st_size > 1_000_000:
        return (rp, "cached", out_file.stat().st_size / 1e6)

    tile_dir = raw_dir(f"jrc_flood/RP{rp}")
    tiffs = sorted(str(t) for t in tile_dir.glob("*.tif") if t.stat().st_size > 1000)
    if not tiffs:
        return (rp, "no tiles", 0)

    cutline = ensure_gha_geojson()

    try:
        gdal_clip_to_cog(tiffs, out_file, cutline=cutline)
        return (rp, "ok", out_file.stat().st_size / 1e6)
    except Exception as e:
        out_file.unlink(missing_ok=True)
        return (rp, f"FAILED: {e}", 0)


def main():
    t_start = time.time()
    ensure_gha_geojson()
    processed_dir("jrc_flood")

    print(f"Processing {len(RETURN_PERIODS)} return periods...\n")

    with ProcessPoolExecutor(max_workers=2) as pool:
        futures = {pool.submit(process_rp, rp): rp for rp in RETURN_PERIODS}
        with tqdm(total=len(futures), desc="JRC Flood COG", unit="rp") as pbar:
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
