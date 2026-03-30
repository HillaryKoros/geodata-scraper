"""Shared config — loads .env, exposes constants and path helpers."""

import os
import shutil
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# Paths
ROOT = Path(__file__).parent
DATA_LOCAL = Path(os.getenv("DATA_LOCAL", ROOT / "data"))
DATA_SSD = Path(os.getenv("DATA_SSD", ""))

# GEE
GEE_KEY = os.getenv("GEE_KEY")
GEE_PROJECT = os.getenv("GEE_PROJECT", "e4drr-crafd")

# PostGIS
PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = os.getenv("PG_PORT", "5432")
PG_DB = os.getenv("PG_DB", "spatialdb")
PG_USER = os.getenv("PG_USER", "koros")
PG_PASSWORD = os.getenv("PG_PASSWORD", "")
PG_URL = f"postgresql://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}"

# AOI — IGAD region bbox (lon_min, lat_min, lon_max, lat_max)
_bbox = os.getenv("AOI_BBOX", "21.0,-5.0,51.5,23.5").split(",")
AOI_BBOX = tuple(float(x) for x in _bbox)

CRS = os.getenv("CRS", "EPSG:4326")

# Gridded ETL
GRIDDED_SOURCE_DIR = Path(
    os.getenv(
        "GRIDDED_SOURCE_DIR",
        "/run/media/koros/PortableSSD1/IGAD-ICPAC/Projects/Hydrology/FloodProofs/data/gridded/IGAD_D2",
    )
)
GRIDDED_PROCESSED_SUBDIR = os.getenv("GRIDDED_PROCESSED_SUBDIR", "gridded/igad_d2")
GRIDDED_MANIFEST_STEM = os.getenv("GRIDDED_MANIFEST_STEM", "igad_d2_manifest")
GRIDDED_ZARR_NAME = os.getenv("GRIDDED_ZARR_NAME", "igad_d2.zarr")
GRIDDED_COG_SUBDIR = os.getenv("GRIDDED_COG_SUBDIR", "gridded/igad_d2_cogs")
GRIDDED_WORKERS = int(os.getenv("GRIDDED_WORKERS", "4"))
GRIDDED_BATCH_SIZE = int(os.getenv("GRIDDED_BATCH_SIZE", "128"))
GRIDDED_TIME_CHUNK = int(os.getenv("GRIDDED_TIME_CHUNK", "64"))
GRIDDED_Y_CHUNK = int(os.getenv("GRIDDED_Y_CHUNK", "256"))
GRIDDED_X_CHUNK = int(os.getenv("GRIDDED_X_CHUNK", "256"))
GRIDDED_COG_LIMIT = int(os.getenv("GRIDDED_COG_LIMIT", "0"))
GRIDDED_COG_VARIABLES = tuple(
    value.strip()
    for value in os.getenv("GRIDDED_COG_VARIABLES", "SM,Discharge,ET").split(",")
    if value.strip()
)
GRIDDED_EXPECTED_VARIABLES = ("SM", "Discharge", "ET")

# IGAD member states (11 countries)
IGAD_COUNTRIES = ["DJI", "ERI", "ETH", "KEN", "SOM", "SSD", "SDN", "UGA", "TZA", "RWA", "BDI"]

ISO3_TO_NAME = {
    "DJI": "Djibouti",
    "ERI": "Eritrea",
    "ETH": "Ethiopia",
    "KEN": "Kenya",
    "SOM": "Somalia",
    "SSD": "South Sudan",
    "SDN": "Sudan",
    "UGA": "Uganda",
    "TZA": "United Republic of Tanzania",
    "RWA": "Rwanda",
    "BDI": "Burundi",
}


def init_ee():
    """Authenticate and initialize Earth Engine."""
    import ee
    credentials = ee.ServiceAccountCredentials(None, key_file=GEE_KEY)
    ee.Initialize(credentials, project=GEE_PROJECT)
    return ee


def raw_dir(subdir: str) -> Path:
    """Get raw data directory, create if needed."""
    d = DATA_LOCAL / "raw" / subdir
    d.mkdir(parents=True, exist_ok=True)
    return d


def processed_dir(subdir: str) -> Path:
    """Get processed data directory, create if needed."""
    d = DATA_LOCAL / "processed" / subdir
    d.mkdir(parents=True, exist_ok=True)
    return d


def sync_to_ssd(subdir: str):
    """Sync a data subdirectory to portable SSD if mounted and writable."""
    if not DATA_SSD:
        print("SSD path not configured, skipping sync")
        return

    try:
        dst_root = Path(DATA_SSD)
        dst_root.mkdir(parents=True, exist_ok=True)
    except (PermissionError, OSError):
        print("SSD not writable, skipping sync")
        return

    src = DATA_LOCAL / subdir
    dst = dst_root / subdir

    if not src.exists():
        return

    dst.mkdir(parents=True, exist_ok=True)

    count = 0
    for f in src.rglob("*"):
        if f.is_file():
            rel = f.relative_to(src)
            target = dst / rel
            target.parent.mkdir(parents=True, exist_ok=True)
            if not target.exists() or f.stat().st_mtime > target.stat().st_mtime:
                shutil.copy2(f, target)
                count += 1

    print(f"synced {count} files to SSD: {dst}")
