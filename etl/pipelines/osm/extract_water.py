"""Extract OSM waterways + water features for GHA via Overpass API.

Downloads rivers, streams, canals, lakes, ponds, reservoirs.
Clips to GHA dissolved boundary.
"""

import json
import time
import urllib.request
import geopandas as gpd
from shapely.geometry import LineString, Polygon
from etl.core.config import processed_dir, raw_dir, sync_to_ssd, AOI_BBOX
from etl.core.utils import load_gha_geom

OVERPASS_URL = "https://overpass-api.de/api/interpreter"


def build_query() -> str:
    """Overpass QL for waterways + water bodies in GHA bbox."""
    s, w, n, e = AOI_BBOX[1], AOI_BBOX[0], AOI_BBOX[3], AOI_BBOX[2]
    return f"""
[out:json][timeout:600];
(
  way["waterway"~"river|stream|canal|drain|ditch"]({s},{w},{n},{e});
  way["natural"="water"]({s},{w},{n},{e});
  relation["natural"="water"]({s},{w},{n},{e});
);
out body;
>;
out skel qt;
"""


def parse_overpass(data: dict) -> gpd.GeoDataFrame:
    """Parse Overpass JSON into GeoDataFrame."""
    nodes = {}
    ways = []

    for elem in data["elements"]:
        if elem["type"] == "node":
            nodes[elem["id"]] = (elem["lon"], elem["lat"])
        elif elem["type"] == "way":
            ways.append(elem)

    features = []
    for way in ways:
        coords = [nodes[nid] for nid in way.get("nodes", []) if nid in nodes]
        if len(coords) < 2:
            continue
        tags = way.get("tags", {})
        feat_type = tags.get("waterway", tags.get("natural", "unknown"))

        # Closed ways → Polygon (lakes, ponds), open → LineString (rivers)
        if len(coords) >= 4 and coords[0] == coords[-1]:
            geom = Polygon(coords)
        else:
            geom = LineString(coords)

        features.append({
            "osm_id": way["id"],
            "feature_type": feat_type,
            "name": tags.get("name", ""),
            "geometry": geom,
        })

    if not features:
        return gpd.GeoDataFrame(columns=["osm_id", "feature_type", "name", "geometry"],
                                geometry="geometry", crs="EPSG:4326")
    return gpd.GeoDataFrame(features, crs="EPSG:4326")


def main():
    t_start = time.time()
    gha_geom = load_gha_geom()
    print("GHA boundary loaded")

    out_file = processed_dir("osm_water") / "osm_water_gha.parquet"
    if out_file.exists() and out_file.stat().st_size > 1000:
        gdf = gpd.read_parquet(out_file)
        print(f"Cached: {len(gdf)} OSM water features")
        return

    raw_file = raw_dir("osm_water") / "overpass_water.json"
    if raw_file.exists() and raw_file.stat().st_size > 1000:
        print("Using cached Overpass response")
        with open(raw_file) as f:
            data = json.load(f)
    else:
        print("Querying Overpass API (may take several minutes)...")
        query = build_query()
        req = urllib.request.Request(OVERPASS_URL, data=f"data={query}".encode(), method="POST")
        with urllib.request.urlopen(req, timeout=660) as resp:
            data = json.loads(resp.read().decode())
        with open(raw_file, "w") as f:
            json.dump(data, f)
        print(f"  {len(data['elements'])} elements")

    print("Parsing water features...")
    gdf = parse_overpass(data)
    print(f"  {len(gdf)} features")

    print("Clipping to GHA...")
    gdf_gha = gpd.clip(gdf, gha_geom)
    print(f"  {len(gdf_gha)} features in GHA")

    gdf_gha.to_parquet(out_file)
    print(f"Saved: {out_file.name} ({out_file.stat().st_size / 1e6:.1f} MB)")

    sync_to_ssd("processed/osm_water")
    print(f"\nDone in {time.time() - t_start:.0f}s")


if __name__ == "__main__":
    main()
