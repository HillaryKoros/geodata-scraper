"""Create dissolved GHA (Greater Horn of Africa) boundary from GADM admin0.

Produces a clean outer boundary — no internal borders, no sliver artifacts.
"""

import time
import geopandas as gpd
from shapely.geometry import MultiPolygon, Polygon
from shapely.ops import unary_union
from shapely.validation import make_valid
from etl.core.config import raw_dir, processed_dir, sync_to_ssd
from etl.core.load import load_parquets


def clean_dissolve(gdf: gpd.GeoDataFrame) -> MultiPolygon:
    """Dissolve country polygons into one clean outer boundary."""
    gdf["geometry"] = gdf["geometry"].apply(make_valid)
    dissolved = make_valid(unary_union(gdf.geometry))

    flat = []
    for g in getattr(dissolved, "geoms", [dissolved]):
        if g.geom_type == "MultiPolygon":
            flat.extend(g.geoms)
        elif g.geom_type == "Polygon":
            flat.append(g)

    # Keep exterior ring only (drops internal borders), filter slivers
    clean_parts = [Polygon(p.exterior) for p in flat if Polygon(p.exterior).area > 0.01]
    return make_valid(MultiPolygon(clean_parts))


def main():
    t_start = time.time()

    adm0_file = raw_dir("gadm") / "igad_adm0.parquet"
    if not adm0_file.exists():
        raise FileNotFoundError("Run extract_gadm.py first")

    out_file = processed_dir("boundaries") / "gha_dissolved.parquet"

    gdf = gpd.read_parquet(adm0_file)
    clean = clean_dissolve(gdf)

    gha = gpd.GeoDataFrame(
        [{"NAME": "Greater Horn of Africa", "NUM_COUNTRIES": len(gdf)}],
        geometry=[clean],
        crs="EPSG:4326",
    )
    out_file.unlink(missing_ok=True)
    gha.to_parquet(out_file)

    n_parts = len(clean.geoms) if hasattr(clean, "geoms") else 1
    print(f"GHA dissolved: {clean.geom_type}, {n_parts} parts, valid={clean.is_valid}")
    print(f"bounds: {[round(x, 1) for x in clean.bounds]}")

    load_parquets([out_file])

    sync_to_ssd("processed/boundaries")
    print(f"Done in {time.time() - t_start:.0f}s")


if __name__ == "__main__":
    main()
