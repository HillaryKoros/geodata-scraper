"""Shared utilities — download, GHA boundary, GDAL raster ops, geometry fixes."""

import re
import subprocess
import urllib.request
import zipfile
import geopandas as gpd
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from shapely.geometry import MultiPolygon, Polygon
from shapely.ops import unary_union

from etl.core.config import processed_dir


# ---------------------------------------------------------------------------
# GHA boundary
# ---------------------------------------------------------------------------

def load_gha() -> gpd.GeoDataFrame:
    """Load the GHA dissolved boundary as a GeoDataFrame."""
    path = processed_dir("boundaries") / "gha_dissolved.parquet"
    if not path.exists():
        raise FileNotFoundError("Run extract_gha_boundary.py first")
    return gpd.read_parquet(path)


def load_gha_geom():
    """Load the GHA dissolved boundary as a single shapely geometry."""
    return load_gha().geometry.iloc[0]


def ensure_gha_geojson() -> Path:
    """Ensure GHA boundary exists as GeoJSON (for GDAL cutline). Returns path."""
    geojson = processed_dir("boundaries") / "gha_dissolved.geojson"
    if not geojson.exists():
        gdf = load_gha()
        gdf.to_file(geojson, driver="GeoJSON")
    return geojson


# ---------------------------------------------------------------------------
# Downloads
# ---------------------------------------------------------------------------

def download_file(url: str, out_file: Path, min_size: int = 1000) -> bool:
    """Download a single file. Skips if already cached above min_size."""
    if out_file.exists() and out_file.stat().st_size > min_size:
        return True
    try:
        urllib.request.urlretrieve(url, out_file)
        if out_file.stat().st_size <= min_size:
            out_file.unlink(missing_ok=True)
            return False
        return True
    except Exception:
        out_file.unlink(missing_ok=True)
        return False


def download_parallel(
    items: list[tuple[str, Path]],
    desc: str = "Download",
    max_workers: int = 4,
) -> tuple[int, int]:
    """Download multiple files in parallel. items = [(url, out_path), ...].
    Returns (ok_count, fail_count)."""
    ok, fail = 0, 0
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {pool.submit(download_file, url, path): path for url, path in items}
        with tqdm(total=len(futures), desc=desc, unit="file") as pbar:
            for fut in as_completed(futures):
                if fut.result():
                    ok += 1
                else:
                    fail += 1
                pbar.update(1)
    return ok, fail


def download_and_unzip(url: str, zip_path: Path, extract_dir: Path) -> Path:
    """Download a zip and extract it. Returns extract_dir."""
    if not zip_path.exists() or zip_path.stat().st_size < 1000:
        print(f"Downloading {zip_path.name}...")
        urllib.request.urlretrieve(url, zip_path)
        print(f"  {zip_path.stat().st_size / 1e6:.0f} MB")
    else:
        print(f"Cached: {zip_path.name} ({zip_path.stat().st_size / 1e6:.0f} MB)")

    if not extract_dir.exists():
        print("Extracting...")
        with zipfile.ZipFile(zip_path) as zf:
            zf.extractall(extract_dir)

    return extract_dir


# ---------------------------------------------------------------------------
# GDAL raster operations
# ---------------------------------------------------------------------------

def gdal_clip_to_cog(
    input_files: list[str],
    out_file: Path,
    cutline: Path | None = None,
    mem_mb: int = 2048,
) -> Path:
    """Mosaic input files via VRT, clip to cutline, output as COG.

    Uses -dstalpha to avoid the nodata fill bug.
    Two-step: gdalwarp → GTiff, then gdal_translate → COG.
    """
    out_dir = out_file.parent
    stem = out_file.stem
    vrt_file = out_dir / f"_tmp_{stem}.vrt"
    tmp_clip = out_dir / f"_tmp_{stem}_clip.tif"

    try:
        # Build VRT
        subprocess.run(
            ["gdalbuildvrt", str(vrt_file)] + input_files,
            check=True, capture_output=True,
        )

        # Warp + clip
        warp_cmd = [
            "gdalwarp", "-of", "GTiff",
            "-t_srs", "EPSG:4326",
            "-dstalpha",
            "-co", "COMPRESS=DEFLATE",
            "-co", "NUM_THREADS=ALL_CPUS",
            "-multi",
            "-wo", "NUM_THREADS=ALL_CPUS",
            "-wm", str(mem_mb),
            "-overwrite",
        ]
        if cutline:
            warp_cmd += ["-cutline", str(cutline), "-crop_to_cutline"]
        warp_cmd += [str(vrt_file), str(tmp_clip)]
        result = subprocess.run(warp_cmd, capture_output=True, text=True)
        if result.returncode != 0:
            raise RuntimeError(f"gdalwarp failed:\n{result.stderr}")

        # Convert to COG
        result = subprocess.run([
            "gdal_translate", "-of", "COG",
            "-co", "COMPRESS=DEFLATE",
            "-co", "NUM_THREADS=ALL_CPUS",
            "-co", "OVERVIEWS=NONE",
            str(tmp_clip), str(out_file),
        ], capture_output=True, text=True)
        if result.returncode != 0:
            raise RuntimeError(f"gdal_translate failed:\n{result.stderr}")

        return out_file
    finally:
        vrt_file.unlink(missing_ok=True)
        tmp_clip.unlink(missing_ok=True)


# ---------------------------------------------------------------------------
# Geometry helpers
# ---------------------------------------------------------------------------

def fix_geometry(geom):
    """Convert GeometryCollection → MultiPolygon by extracting polygons."""
    if geom.geom_type in ("Polygon", "MultiPolygon"):
        return geom
    if geom.geom_type == "GeometryCollection":
        polys = [g for g in geom.geoms if isinstance(g, (Polygon, MultiPolygon))]
        if not polys:
            return geom
        return unary_union(polys)
    return geom


# ---------------------------------------------------------------------------
# Tile grid helpers
# ---------------------------------------------------------------------------

def parse_ns_ew_tile(filename: str) -> tuple[int, int] | None:
    """Parse lat/lon from tile names like ID150_N10_E30_RP100_depth.tif.
    Returns (lat_val, lon_val) or None."""
    m = re.search(r'_([NS])(\d+)_([EW])(\d+)_', filename)
    if not m:
        return None
    ns, lat, ew, lon = m.groups()
    lat_val = int(lat) * (1 if ns == "N" else -1)
    lon_val = int(lon) * (1 if ew == "E" else -1)
    return (lat_val, lon_val)


def tiles_in_bbox(
    filenames: list[str],
    bbox: tuple[float, float, float, float],
    tile_size: int = 10,
) -> list[str]:
    """Filter tile filenames that intersect a bbox (lon_min, lat_min, lon_max, lat_max).

    JRC tiles: lat_val is the UPPER edge, lon_val is the LEFT edge.
    So tile covers lat (lat_val - tile_size) to lat_val, lon lon_val to (lon_val + tile_size).
    """
    lon_min, lat_min, lon_max, lat_max = bbox
    result = []
    for f in filenames:
        parsed = parse_ns_ew_tile(f)
        if parsed is None:
            continue
        lat_val, lon_val = parsed
        tile_lat_min = lat_val - tile_size
        tile_lat_max = lat_val
        tile_lon_min = lon_val
        tile_lon_max = lon_val + tile_size
        if (tile_lon_max > lon_min and tile_lon_min < lon_max
                and tile_lat_max > lat_min and tile_lat_min < lat_max):
            result.append(f)
    return result
