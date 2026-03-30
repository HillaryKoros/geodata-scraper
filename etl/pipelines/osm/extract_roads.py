"""Extract OSM road network for GHA via Overpass API.

Downloads all highways (motorway → residential) within GHA bbox.
Clips to GHA dissolved boundary.
"""

import json
import time
import urllib.request
import geopandas as gpd
from shapely.geometry import LineString
from etl.core.config import processed_dir, raw_dir, sync_to_ssd, AOI_BBOX
from etl.core.utils import load_gha_geom

OVERPASS_URL = "https://overpass-api.de/api/interpreter"


def build_query() -> str:
    s, w, n, e = AOI_BBOX[1], AOI_BBOX[0], AOI_BBOX[3], AOI_BBOX[2]
    return f"""
[out:json][timeout:900];
(
  way["highway"~"motorway|trunk|primary|secondary|tertiary|residential"]({s},{w},{n},{e});
);
out body;
>;
out skel qt;
"""


def parse_roads(data: dict) -> gpd.GeoDataFrame:
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
        features.append({
            "osm_id": way["id"],
            "highway": tags.get("highway", ""),
            "name": tags.get("name", ""),
            "surface": tags.get("surface", ""),
            "geometry": LineString(coords),
        })

    if not features:
        return gpd.GeoDataFrame(columns=["osm_id", "highway", "name", "surface", "geometry"],
                                geometry="geometry", crs="EPSG:4326")
    return gpd.GeoDataFrame(features, crs="EPSG:4326")


def main():
    t_start = time.time()
    gha_geom = load_gha_geom()
    print("GHA boundary loaded")

    out_file = processed_dir("osm_roads") / "osm_roads_gha.parquet"
    if out_file.exists() and out_file.stat().st_size > 1000:
        gdf = gpd.read_parquet(out_file)
        print(f"Cached: {len(gdf)} OSM roads")
        return

    raw_file = raw_dir("osm_roads") / "overpass_roads.json"
    if raw_file.exists() and raw_file.stat().st_size > 1000:
        print("Using cached Overpass response")
        with open(raw_file) as f:
            data = json.load(f)
    else:
        print("Querying Overpass API (large query, may take 10+ minutes)...")
        query = build_query()
        req = urllib.request.Request(OVERPASS_URL, data=f"data={query}".encode(), method="POST")
        with urllib.request.urlopen(req, timeout=960) as resp:
            data = json.loads(resp.read().decode())
        with open(raw_file, "w") as f:
            json.dump(data, f)
        print(f"  {len(data['elements'])} elements")

    print("Parsing roads...")
    gdf = parse_roads(data)
    print(f"  {len(gdf)} road segments")

    print("Clipping to GHA...")
    gdf_gha = gpd.clip(gdf, gha_geom)
    print(f"  {len(gdf_gha)} segments in GHA")

    gdf_gha.to_parquet(out_file)
    print(f"Saved: {out_file.name} ({out_file.stat().st_size / 1e6:.1f} MB)")

    sync_to_ssd("processed/osm_roads")
    print(f"\nDone in {time.time() - t_start:.0f}s")


if __name__ == "__main__":
    main()
