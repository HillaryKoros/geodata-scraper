"""Shared helpers for gridded NetCDF ingestion.

The gridded source folder contains gzipped NetCDF outputs named like:
    hmc.output-grid.YYYYMMDDHHMM.nc.gz

This module keeps the parsing, validation, and metadata extraction logic in one
place so the manifest step and future stack/export steps can reuse it.
"""

from __future__ import annotations

import gzip
import importlib.util
import json
import re
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import numpy as np
from netCDF4 import Dataset


def _load_core_config():
    config_path = Path(__file__).resolve().parents[2] / "core" / "config.py"
    spec = importlib.util.spec_from_file_location("spatial_db_core_config", config_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot load core config from {config_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


_CORE_CONFIG = _load_core_config()
processed_dir = _CORE_CONFIG.processed_dir
raw_dir = _CORE_CONFIG.raw_dir
sync_to_ssd = _CORE_CONFIG.sync_to_ssd

TIMESTAMP_RE = re.compile(r"^hmc\.output-grid\.(\d{12})\.nc\.gz$")
TIMESTAMP_FMT = "%Y%m%d%H%M"
DEFAULT_SOURCE_DIR = Path(getattr(_CORE_CONFIG, "GRIDDED_SOURCE_DIR"))
OUTPUT_SUBDIR = getattr(_CORE_CONFIG, "GRIDDED_PROCESSED_SUBDIR", "gridded")
MANIFEST_STEM = getattr(_CORE_CONFIG, "GRIDDED_MANIFEST_STEM", "igad_d2_manifest")
ZARR_STORE_NAME = getattr(_CORE_CONFIG, "GRIDDED_ZARR_NAME", "igad_d2.zarr")
COG_SUBDIR = getattr(_CORE_CONFIG, "GRIDDED_COG_SUBDIR", "gridded/igad_d2_cogs")
DEFAULT_WORKERS = int(getattr(_CORE_CONFIG, "GRIDDED_WORKERS", 4))
DEFAULT_BATCH_SIZE = int(getattr(_CORE_CONFIG, "GRIDDED_BATCH_SIZE", 128))
DEFAULT_TIME_CHUNK = int(getattr(_CORE_CONFIG, "GRIDDED_TIME_CHUNK", 64))
DEFAULT_Y_CHUNK = int(getattr(_CORE_CONFIG, "GRIDDED_Y_CHUNK", 256))
DEFAULT_X_CHUNK = int(getattr(_CORE_CONFIG, "GRIDDED_X_CHUNK", 256))
DEFAULT_COG_LIMIT = int(getattr(_CORE_CONFIG, "GRIDDED_COG_LIMIT", 0))
EXPECTED_VARIABLES = tuple(getattr(_CORE_CONFIG, "GRIDDED_EXPECTED_VARIABLES", ("SM", "Discharge", "ET")))
DEFAULT_COG_VARIABLES = tuple(getattr(_CORE_CONFIG, "GRIDDED_COG_VARIABLES", EXPECTED_VARIABLES))


@dataclass(slots=True)
class GridMetadata:
    """Canonical metadata extracted from a valid gridded file."""

    nrows: int | None = None
    ncols: int | None = None
    xllcorner: float | None = None
    yllcorner: float | None = None
    xcellsize: float | None = None
    ycellsize: float | None = None
    lon_min: float | None = None
    lon_max: float | None = None
    lat_min: float | None = None
    lat_max: float | None = None
    south_north: int | None = None
    west_east: int | None = None
    time_dim: int | None = None
    time_str_length: int | None = None
    day1_steps: int | None = None


@dataclass(slots=True)
class FileInspection:
    """Result of validating and inspecting one gzipped NetCDF file."""

    file_name: str
    file_path: str
    size_bytes: int
    timestamp: str | None
    timestamp_iso: str | None
    status: str
    error_type: str | None = None
    error_message: str | None = None
    gzip_ok: bool = False
    netcdf_ok: bool = False
    data_model: str | None = None
    dimensions_json: str | None = None
    variables_json: str | None = None
    global_attrs_json: str | None = None
    grid_json: str | None = None
    expected_variables_json: str | None = None
    present_variables_json: str | None = None
    missing_variables_json: str | None = None
    time_coverage_end: str | None = None


def source_dir() -> Path:
    """Return the configured gridded source directory."""
    return DEFAULT_SOURCE_DIR


def output_dir(subdir: str | None = None) -> Path:
    """Return the processed output directory for gridded products."""
    return processed_dir(subdir or OUTPUT_SUBDIR)


def raw_cache_dir() -> Path:
    """Return a raw cache directory for future gridded staging if needed."""
    return raw_dir(OUTPUT_SUBDIR)


def sync_output_dir(subdir: str | None = None) -> None:
    """Best-effort SSD sync for gridded processed outputs."""
    sync_to_ssd(f"processed/{subdir or OUTPUT_SUBDIR}")


def parse_timestamp(path: Path | str) -> datetime | None:
    """Parse the timestamp embedded in a file name."""
    name = Path(path).name
    match = TIMESTAMP_RE.match(name)
    if not match:
        return None
    return datetime.strptime(match.group(1), TIMESTAMP_FMT)


def timestamp_key(path: Path | str) -> str | None:
    """Return the canonical timestamp key from the file name."""
    ts = parse_timestamp(path)
    if ts is None:
        return None
    return ts.strftime(TIMESTAMP_FMT)


def json_safe(value: Any) -> Any:
    """Convert NumPy/Python values into JSON-safe primitives."""
    if isinstance(value, dict):
        return {str(k): json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [json_safe(v) for v in value]
    if isinstance(value, (np.integer, np.floating)):
        return value.item()
    if isinstance(value, np.ndarray):
        return value.tolist()
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    if isinstance(value, Path):
        return str(value)
    if hasattr(value, "tolist") and not isinstance(value, (str, bytes)):
        try:
            return value.tolist()
        except Exception:
            return str(value)
    return value


def to_json_text(value: Any) -> str:
    """Serialize a value into compact JSON."""
    return json.dumps(json_safe(value), sort_keys=True, separators=(",", ":"))


def load_payload(path: Path) -> bytes:
    """Fully decompress a gzipped NetCDF file into memory."""
    with gzip.open(path, "rb") as handle:
        return handle.read()


def open_dataset_from_gzip(path: Path) -> Dataset:
    """Open a gzipped NetCDF file using an in-memory buffer."""
    payload = load_payload(path)
    return Dataset("inmemory.nc", memory=payload)


def _attr_dict(obj: Any) -> dict[str, Any]:
    """Extract object attributes as a JSON-safe mapping."""
    attrs = {}
    for key in getattr(obj, "ncattrs", lambda: [])():
        attrs[key] = json_safe(getattr(obj, key))
    return attrs


def _var_dict(var) -> dict[str, Any]:
    """Extract variable metadata in a compact JSON-safe form."""
    attrs = {
        "dtype": str(var.dtype),
        "dimensions": list(var.dimensions),
        "shape": list(var.shape),
    }
    for key in getattr(var, "ncattrs", lambda: [])():
        attrs[key] = json_safe(getattr(var, key))
    return attrs


def extract_grid_metadata(ds: Dataset) -> GridMetadata:
    """Derive canonical grid metadata from a NetCDF dataset."""
    dims = ds.dimensions
    vars_ = ds.variables

    lon = vars_.get("Longitude")
    lat = vars_.get("Latitude")

    lon_min = lon_max = lat_min = lat_max = None
    if lon is not None:
        lon_arr = np.asarray(lon[:])
        lon_min = float(np.nanmin(lon_arr))
        lon_max = float(np.nanmax(lon_arr))
    if lat is not None:
        lat_arr = np.asarray(lat[:])
        lat_min = float(np.nanmin(lat_arr))
        lat_max = float(np.nanmax(lat_arr))

    return GridMetadata(
        nrows=len(dims["south_north"]) if "south_north" in dims else None,
        ncols=len(dims["west_east"]) if "west_east" in dims else None,
        xllcorner=json_safe(getattr(ds, "xllcorner", None)),
        yllcorner=json_safe(getattr(ds, "yllcorner", None)),
        xcellsize=json_safe(getattr(ds, "xcellsize", None)),
        ycellsize=json_safe(getattr(ds, "ycellsize", None)),
        lon_min=lon_min,
        lon_max=lon_max,
        lat_min=lat_min,
        lat_max=lat_max,
        south_north=len(dims["south_north"]) if "south_north" in dims else None,
        west_east=len(dims["west_east"]) if "west_east" in dims else None,
        time_dim=len(dims["time"]) if "time" in dims else None,
        time_str_length=(
            len(dims["time_str_length"]) if "time_str_length" in dims else None
        ),
        day1_steps=len(dims["day1_steps"]) if "day1_steps" in dims else None,
    )


def inspect_dataset(ds: Dataset) -> dict[str, Any]:
    """Collect dataset-level metadata for the manifest."""
    grid = extract_grid_metadata(ds)
    variables = {name: _var_dict(var) for name, var in ds.variables.items()}
    expected = list(EXPECTED_VARIABLES)
    present = [name for name in expected if name in ds.variables]
    missing = [name for name in expected if name not in ds.variables]

    return {
        "data_model": getattr(ds, "data_model", None),
        "dimensions": {name: len(dim) for name, dim in ds.dimensions.items()},
        "variables": variables,
        "global_attrs": _attr_dict(ds),
        "grid": asdict(grid),
        "expected_variables": expected,
        "present_variables": present,
        "missing_variables": missing,
        "time_coverage_end": json_safe(getattr(ds, "time_coverage_end", None)),
    }


def _json_or_none(value: Any) -> str | None:
    """Serialize a value if present."""
    if value is None:
        return None
    return to_json_text(value)


def build_record(
    path: Path,
    status: str,
    *,
    error_type: str | None = None,
    error_message: str | None = None,
    gzip_ok: bool = False,
    netcdf_ok: bool = False,
    metadata: dict[str, Any] | None = None,
) -> FileInspection:
    """Build a manifest record from validation output."""
    ts = parse_timestamp(path)
    ts_key = ts.strftime(TIMESTAMP_FMT) if ts is not None else None
    ts_iso = ts.isoformat(sep=" ") if ts is not None else None

    payload = metadata or {}
    return FileInspection(
        file_name=path.name,
        file_path=str(path),
        size_bytes=path.stat().st_size if path.exists() else 0,
        timestamp=ts_key,
        timestamp_iso=ts_iso,
        status=status,
        error_type=error_type,
        error_message=error_message,
        gzip_ok=gzip_ok,
        netcdf_ok=netcdf_ok,
        data_model=payload.get("data_model"),
        dimensions_json=_json_or_none(payload.get("dimensions")),
        variables_json=_json_or_none(payload.get("variables")),
        global_attrs_json=_json_or_none(payload.get("global_attrs")),
        grid_json=_json_or_none(payload.get("grid")),
        expected_variables_json=_json_or_none(payload.get("expected_variables")),
        present_variables_json=_json_or_none(payload.get("present_variables")),
        missing_variables_json=_json_or_none(payload.get("missing_variables")),
        time_coverage_end=payload.get("time_coverage_end"),
    )
