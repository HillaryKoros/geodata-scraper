"""Pre-compute static flood impact per admin2 district per return period.

Reads JRC flood (water removed), WorldPop population, ESA WorldCover cropland,
and GADM admin2 boundaries. Outputs ibf_impact_table.parquet.

Usage:
    python -m etl.pipelines.ibf.impact_static
    python -m etl.cli extract ibf-impact
"""

import sys
import time

import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio
from rasterio.features import rasterize

from etl.core.config import processed_dir, raw_dir, sync_to_ssd
from etl.pipelines.ibf.config import RETURN_PERIODS

PIXEL_AREA_KM2 = 0.0086  # ~93m × 92m at GHA latitudes


def compute_impact_table() -> pd.DataFrame:
    """Build the full impact table from rasters + admin2 boundaries."""
    sys.stdout = open(sys.stdout.fileno(), mode="w", buffering=1)

    t0 = time.time()

    # Load admin2 boundaries
    adm2_path = raw_dir("gadm") / "igad_adm2.parquet"
    if not adm2_path.exists():
        raise FileNotFoundError(f"Admin2 boundaries not found: {adm2_path}")
    adm2 = gpd.read_parquet(adm2_path)
    n_districts = len(adm2)
    print(f"Admin2 districts: {n_districts}")

    # Reference grid from flood raster
    flood_ref = processed_dir("jrc_flood") / "jrc_flood_rp100_nowater_gha.tif"
    if not flood_ref.exists():
        raise FileNotFoundError(f"Flood raster not found: {flood_ref}")

    with rasterio.open(flood_ref) as ref:
        ref_transform = ref.transform
        ref_width = ref.width
        ref_height = ref.height

    # Rasterize admin2 districts
    print("Rasterizing admin2...", flush=True)
    shapes = [(geom, idx) for idx, geom in enumerate(adm2.geometry)]
    admin_raster = rasterize(
        shapes,
        out_shape=(ref_height, ref_width),
        transform=ref_transform,
        fill=-1,
        dtype=np.int32,
    ).ravel()
    valid_mask = admin_raster >= 0
    print(f"  {np.count_nonzero(valid_mask)} pixels assigned ({time.time() - t0:.0f}s)", flush=True)

    # Load population
    print("Loading population...", flush=True)
    pop_path = processed_dir("worldpop") / "worldpop_2020_gha.tif"
    if not pop_path.exists():
        raise FileNotFoundError(f"Population raster not found: {pop_path}")
    with rasterio.open(pop_path) as src:
        pop = src.read(1)
    pop[pop <= 0] = 0
    pop[pop > 100000] = 0
    flat_pop = pop.ravel()

    # Pre-compute total pop per district
    pop_total = np.bincount(admin_raster[valid_mask], weights=flat_pop[valid_mask], minlength=n_districts)
    pixels_per = np.bincount(admin_raster[valid_mask], minlength=n_districts)
    print(f"  Total pop: {pop_total.sum() / 1e6:.1f}M ({time.time() - t0:.0f}s)", flush=True)

    # Load cropland (optional)
    crop_path = processed_dir("worldcover") / "esa_worldcover_cropland_gha.tif"
    has_cropland = crop_path.exists()
    if has_cropland:
        with rasterio.open(crop_path) as src:
            crop_binary = (src.read(1) > 0.3).ravel()
        print("Cropland loaded", flush=True)

    # Process each return period
    results = []
    for rp in RETURN_PERIODS:
        print(f"RP{rp}...", flush=True)
        flood_path = processed_dir("jrc_flood") / f"jrc_flood_rp{rp}_nowater_gha.tif"
        if not flood_path.exists():
            print(f"  SKIP — {flood_path} not found")
            continue

        with rasterio.open(flood_path) as src:
            flood = src.read(1)
            flood_nodata = src.nodata

        flooded = ((flood != flood_nodata) & (flood > 0)).ravel()
        flat_flood = flood.ravel()
        both = valid_mask & flooded

        pop_exposed = np.bincount(admin_raster[both], weights=flat_pop[both], minlength=n_districts)
        flood_px = np.bincount(admin_raster[both], minlength=n_districts)
        depth_sum = np.bincount(admin_raster[both], weights=flat_flood[both], minlength=n_districts)

        if has_cropland:
            crop_flood = valid_mask & flooded & crop_binary
            crop_px = np.bincount(admin_raster[crop_flood], minlength=n_districts)
        else:
            crop_px = np.zeros(n_districts)

        for idx in range(n_districts):
            row = adm2.iloc[idx]
            pt = pop_total[idx]
            pe = pop_exposed[idx]
            fp = flood_px[idx]
            tp = pixels_per[idx]

            results.append({
                "country": row.get("COUNTRY", row.get("NAME_0", "")),
                "admin1": row.get("NAME_1", ""),
                "admin2": row.get("NAME_2", ""),
                "gid_2": row.get("GID_2", ""),
                "rp": rp,
                "pop_total": int(pt),
                "pop_exposed": int(pe),
                "pop_exposed_pct": round(pe / pt * 100, 1) if pt > 0 else 0,
                "flood_area_km2": round(fp * PIXEL_AREA_KM2, 1),
                "area_flooded_pct": round(fp / tp * 100, 1) if tp > 0 else 0,
                "mean_depth_m": round(depth_sum[idx] / fp, 2) if fp > 0 else 0,
                "cropland_flooded_km2": round(crop_px[idx] * PIXEL_AREA_KM2, 1),
            })

        total_exposed = pop_exposed.sum()
        print(f"  {total_exposed / 1e6:.1f}M exposed ({time.time() - t0:.0f}s)", flush=True)

    return pd.DataFrame(results)


def main():
    """Run static impact pre-computation and save outputs."""
    t0 = time.time()
    df = compute_impact_table()

    out_dir = processed_dir("ibf")
    csv_path = out_dir / "ibf_impact_table.csv"
    parquet_path = out_dir / "ibf_impact_table.parquet"

    df.to_csv(csv_path, index=False)
    df.to_parquet(parquet_path, index=False)

    # Also save to the top-level processed dir for backwards compat
    df.to_csv(processed_dir("") / "ibf_impact_table.csv", index=False)
    df.to_parquet(processed_dir("") / "ibf_impact_table.parquet", index=False)

    n_districts = df["gid_2"].nunique()
    n_rps = df["rp"].nunique()
    print(f"\nImpact table: {len(df)} rows ({n_districts} districts x {n_rps} RPs)")
    print(f"Saved: {csv_path}")
    print(f"Saved: {parquet_path}")

    sync_to_ssd("processed/ibf")
    print(f"Done in {time.time() - t0:.0f}s")


if __name__ == "__main__":
    main()
