"""Gridded NetCDF.gz -> GeoZarr stack writer.

This module consumes a manifest of source files, probes readable timesteps,
and writes a chunked xarray-compatible Zarr store with north-up geospatial
coordinates.
"""

from __future__ import annotations

import argparse
import csv
import gzip
import json
import re
import shutil
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timezone
from itertools import repeat
from pathlib import Path
from typing import Any, Iterable

import numpy as np
try:
    import pyarrow.parquet as pq
except ImportError:
    pq = None
import xarray as xr
import zarr
from numcodecs import Blosc
from netCDF4 import Dataset
from rasterio.crs import CRS
from rasterio.transform import Affine

import importlib.util

_CORE_CONFIG_PATH = Path(__file__).resolve().parents[2] / "core" / "config.py"
_CORE_CONFIG_SPEC = importlib.util.spec_from_file_location("spatial_db_core_config", _CORE_CONFIG_PATH)
if _CORE_CONFIG_SPEC is None or _CORE_CONFIG_SPEC.loader is None:
    raise ImportError(f"Cannot load core config from {_CORE_CONFIG_PATH}")
_CORE_CONFIG = importlib.util.module_from_spec(_CORE_CONFIG_SPEC)
_CORE_CONFIG_SPEC.loader.exec_module(_CORE_CONFIG)
processed_dir = _CORE_CONFIG.processed_dir
sync_to_ssd = _CORE_CONFIG.sync_to_ssd

VAR_NAMES = tuple(getattr(_CORE_CONFIG, "GRIDDED_EXPECTED_VARIABLES", ("SM", "Discharge", "ET")))
FILE_RE = re.compile(r"^hmc\.output-grid\.(\d{12})\.nc\.gz$")
DEFAULT_SOURCE_DIR = Path(getattr(_CORE_CONFIG, "GRIDDED_SOURCE_DIR"))
DEFAULT_MANIFEST_NAMES = (
    f"{getattr(_CORE_CONFIG, 'GRIDDED_MANIFEST_STEM', 'igad_d2_manifest')}.parquet",
    f"{getattr(_CORE_CONFIG, 'GRIDDED_MANIFEST_STEM', 'igad_d2_manifest')}.csv",
    "gridded_manifest.parquet",
    "gridded_manifest.csv",
    "manifest.parquet",
    "manifest.csv",
)
DEFAULT_STORE_NAME = getattr(_CORE_CONFIG, "GRIDDED_ZARR_NAME", "igad_d2.zarr")
DEFAULT_BATCH_SIZE = int(getattr(_CORE_CONFIG, "GRIDDED_BATCH_SIZE", 128))
DEFAULT_WORKERS = int(getattr(_CORE_CONFIG, "GRIDDED_WORKERS", 4))
DEFAULT_TIME_CHUNK = int(getattr(_CORE_CONFIG, "GRIDDED_TIME_CHUNK", 64))
DEFAULT_Y_CHUNK = int(getattr(_CORE_CONFIG, "GRIDDED_Y_CHUNK", 256))
DEFAULT_X_CHUNK = int(getattr(_CORE_CONFIG, "GRIDDED_X_CHUNK", 256))


@dataclass(frozen=True)
class ManifestRecord:
    source_path: Path
    timestamp: datetime
    status: str = "ok"
    message: str = ""
    extra: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class GridSpec:
    nrows: int
    ncols: int
    xllcorner: float
    yllcorner: float
    cellsize_x: float
    cellsize_y: float
    transform: Affine
    crs: CRS
    x: np.ndarray
    y: np.ndarray


@dataclass(frozen=True)
class ProbeResult:
    record: ManifestRecord
    ok: bool
    message: str = ""
    grid_spec: GridSpec | None = None
    var_attrs: dict[str, dict[str, Any]] = field(default_factory=dict)


@dataclass(frozen=True)
class BatchPayload:
    records: list[ManifestRecord]
    times: np.ndarray
    arrays: dict[str, np.ndarray]
    messages: list[str] = field(default_factory=list)


def default_output_dir() -> Path:
    return processed_dir(getattr(_CORE_CONFIG, "GRIDDED_PROCESSED_SUBDIR", "gridded"))


def parse_timestamp(value: str | datetime) -> datetime:
    if isinstance(value, datetime):
        return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)
    text = str(value).strip()
    if text.endswith("Z"):
        text = text[:-1]
    if "T" in text:
        dt = datetime.fromisoformat(text)
        return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt.astimezone(timezone.utc)
    if re.fullmatch(r"\d{12}", text):
        return datetime.strptime(text, "%Y%m%d%H%M").replace(tzinfo=timezone.utc)
    if re.fullmatch(r"\d{8}", text):
        return datetime.strptime(text, "%Y%m%d").replace(tzinfo=timezone.utc)
    dt = datetime.fromisoformat(text)
    return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt.astimezone(timezone.utc)


def timestamp_from_name(name: str) -> datetime:
    match = FILE_RE.match(name)
    if not match:
        raise ValueError(f"Unsupported filename: {name}")
    return datetime.strptime(match.group(1), "%Y%m%d%H%M").replace(tzinfo=timezone.utc)


def utc_seconds(value: datetime) -> int:
    return int(value.astimezone(timezone.utc).timestamp())


def _normalize_status(value: Any) -> str:
    text = str(value).strip().lower()
    if text in {"1", "true", "t", "yes", "y", "ok", "good", "valid", "passed"}:
        return "ok"
    if text in {"0", "false", "f", "no", "n", "bad", "invalid", "failed"}:
        return "bad"
    return text or "ok"


def _resolve_source_path(value: Any, base_dir: Path | None = None) -> Path | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    path = Path(text)
    if path.is_absolute():
        return path
    if base_dir is not None:
        candidate = base_dir / path
        if candidate.exists():
            return candidate
    if DEFAULT_SOURCE_DIR.exists():
        candidate = DEFAULT_SOURCE_DIR / path
        if candidate.exists():
            return candidate
    return path


def _row_to_record(row: dict[str, Any], base_dir: Path | None = None) -> ManifestRecord | None:
    path = None
    for key in ("source_path", "path", "file_path", "filename", "file", "name"):
        value = row.get(key)
        if value:
            path = _resolve_source_path(value, base_dir=base_dir)
            if path is not None:
                break
    if path is None:
        return None

    timestamp = None
    for key in ("timestamp", "time", "datetime", "date", "timestep"):
        value = row.get(key)
        if not value:
            continue
        try:
            timestamp = parse_timestamp(value)
            break
        except Exception:
            continue
    if timestamp is None:
        try:
            timestamp = timestamp_from_name(path.name)
        except Exception:
            return None

    status = "ok"
    for key in ("status", "ok", "is_ok", "valid", "state"):
        value = row.get(key)
        if value is not None and value != "":
            status = _normalize_status(value)
            break

    message = ""
    for key in ("message", "reason", "error", "note"):
        value = row.get(key)
        if value:
            message = str(value)
            break

    ignored = {"source_path", "path", "file_path", "filename", "file", "name", "timestamp", "time", "datetime", "date", "timestep", "status", "ok", "is_ok", "valid", "state", "message", "reason", "error", "note"}
    extra = {k: v for k, v in row.items() if k not in ignored}
    return ManifestRecord(source_path=path, timestamp=timestamp, status=status, message=message, extra=extra)


def load_manifest(path: Path) -> list[ManifestRecord]:
    if not path.exists():
        return []
    if path.suffix.lower() in {".parquet", ".pq"}:
        if pq is None:
            raise ImportError("pyarrow is required to read parquet manifests")
        table = pq.read_table(path)
        return [rec for row in table.to_pylist() if (rec := _row_to_record(row, base_dir=path.parent)) is not None]
    if path.suffix.lower() == ".csv":
        with path.open("r", newline="") as fh:
            reader = csv.DictReader(fh)
            return [rec for row in reader if (rec := _row_to_record(row, base_dir=path.parent)) is not None]
    raise ValueError(f"Unsupported manifest format: {path}")


def discover_manifest(manifest: Path | None = None) -> Path | None:
    if manifest is not None and manifest.exists():
        return manifest
    root = default_output_dir()
    for name in DEFAULT_MANIFEST_NAMES:
        candidate = root / name
        if candidate.exists():
            return candidate
    return None


def scan_source_dir(source_dir: Path) -> list[ManifestRecord]:
    records: list[ManifestRecord] = []
    for path in sorted(source_dir.glob("*.nc.gz")):
        try:
            timestamp = timestamp_from_name(path.name)
        except Exception:
            continue
        records.append(ManifestRecord(source_path=path, timestamp=timestamp, status="ok"))
    return records


def resolve_input_records(manifest: Path | None = None, source_dir: Path | None = None) -> list[ManifestRecord]:
    manifest_path = discover_manifest(manifest=manifest)
    if manifest_path is not None:
        return load_manifest(manifest_path)
    src = source_dir or DEFAULT_SOURCE_DIR
    if src.exists():
        return scan_source_dir(src)
    return []


def filter_candidate_records(records: Iterable[ManifestRecord]) -> list[ManifestRecord]:
    filtered: list[ManifestRecord] = []
    for record in records:
        if _normalize_status(record.status) != "ok":
            continue
        if record.source_path.exists() and record.source_path.stat().st_size > 0:
            filtered.append(record)
    filtered.sort(key=lambda item: item.timestamp)
    return filtered


def _read_nc_bytes(path: Path) -> bytes:
    with gzip.open(path, "rb") as fh:
        return fh.read()


def _open_dataset_from_gz(path: Path) -> Dataset:
    return Dataset("inmemory.nc", memory=_read_nc_bytes(path))


def _read_var_array(var) -> np.ndarray:
    raw = var[:]
    if np.ma.isMaskedArray(raw):
        arr = raw.filled(np.nan)
    else:
        arr = np.asarray(raw)
    return np.array(arr, dtype=np.float32, copy=True)


def _replace_sentinels(arr: np.ndarray, name: str, attrs: dict[str, Any]) -> np.ndarray:
    sentinels: list[float] = []
    for key in ("_FillValue", "missing_value"):
        value = attrs.get(key)
        if value is None:
            continue
        try:
            value = float(value)
        except Exception:
            continue
        if np.isfinite(value):
            sentinels.append(value)
    if name.upper() == "SM":
        sentinels.extend([-9999.0, -32768.0])
    for sentinel in sentinels:
        arr = np.where(np.isclose(arr, sentinel), np.nan, arr)
    return arr


def _extract_var_attrs(var) -> dict[str, Any]:
    attrs: dict[str, Any] = {}
    for key in var.ncattrs():
        value = getattr(var, key)
        if isinstance(value, np.generic):
            value = value.item()
        attrs[key] = value
    return attrs


def extract_grid_spec(ds: Dataset) -> GridSpec:
    nrows = int(getattr(ds, "nrows"))
    ncols = int(getattr(ds, "ncols"))
    xllcorner = float(getattr(ds, "xllcorner"))
    yllcorner = float(getattr(ds, "yllcorner"))
    cellsize_x = float(getattr(ds, "xcellsize"))
    cellsize_y = float(getattr(ds, "ycellsize"))
    north = yllcorner + nrows * cellsize_y
    transform = Affine(cellsize_x, 0.0, xllcorner, 0.0, -cellsize_y, north)
    x = xllcorner + (np.arange(ncols, dtype=np.float64) + 0.5) * cellsize_x
    y = north - (np.arange(nrows, dtype=np.float64) + 0.5) * cellsize_y
    return GridSpec(
        nrows=nrows,
        ncols=ncols,
        xllcorner=xllcorner,
        yllcorner=yllcorner,
        cellsize_x=cellsize_x,
        cellsize_y=cellsize_y,
        transform=transform,
        crs=CRS.from_epsg(4326),
        x=x,
        y=y,
    )


def probe_record(record: ManifestRecord, expected_shape: tuple[int, int] | None = None) -> ProbeResult:
    try:
        ds = _open_dataset_from_gz(record.source_path)
    except Exception as exc:
        return ProbeResult(record=record, ok=False, message=f"{type(exc).__name__}: {exc}")

    try:
        missing = [name for name in VAR_NAMES if name not in ds.variables]
        if missing:
            return ProbeResult(record=record, ok=False, message=f"missing variables: {', '.join(missing)}")

        grid_spec = extract_grid_spec(ds)
        if expected_shape is not None and (grid_spec.nrows, grid_spec.ncols) != expected_shape:
            return ProbeResult(record=record, ok=False, message=f"shape mismatch: expected {expected_shape}, got {(grid_spec.nrows, grid_spec.ncols)}", grid_spec=grid_spec)

        var_attrs = {name: _extract_var_attrs(ds.variables[name]) for name in VAR_NAMES}
        return ProbeResult(record=record, ok=True, grid_spec=grid_spec, var_attrs=var_attrs)
    finally:
        ds.close()


def probe_records(records: list[ManifestRecord], workers: int = DEFAULT_WORKERS) -> tuple[list[ManifestRecord], GridSpec, dict[str, dict[str, Any]], list[ProbeResult]]:
    if not records:
        raise FileNotFoundError("No gridded source records were found")

    first_probe: ProbeResult | None = None
    first_index = -1
    for idx, record in enumerate(records):
        probe = probe_record(record)
        if probe.ok and probe.grid_spec is not None:
            first_probe = probe
            first_index = idx
            break
    if first_probe is None:
        raise RuntimeError("Cannot determine grid spec from any readable source file")

    grid_spec = first_probe.grid_spec
    readable: list[ManifestRecord] = [records[first_index]]
    results: list[ProbeResult] = [first_probe]
    if len(records) == 1:
        return readable, grid_spec, first_probe.var_attrs, results

    ordered = records[:first_index] + records[first_index + 1 :]
    with ProcessPoolExecutor(max_workers=max(1, workers)) as pool:
        for result in pool.map(probe_record, ordered, repeat((grid_spec.nrows, grid_spec.ncols))):
            results.append(result)
            if result.ok:
                readable.append(result.record)

    readable.sort(key=lambda item: item.timestamp)
    return readable, grid_spec, first_probe.var_attrs, results


def chunk_records(records: list[ManifestRecord], batch_size: int) -> list[list[ManifestRecord]]:
    return [records[i : i + batch_size] for i in range(0, len(records), batch_size)]


def read_batch(batch: list[ManifestRecord], grid_spec: GridSpec) -> BatchPayload:
    times: list[int] = []
    per_var: dict[str, list[np.ndarray]] = {name: [] for name in VAR_NAMES}
    kept: list[ManifestRecord] = []
    messages: list[str] = []

    for record in batch:
        try:
            ds = _open_dataset_from_gz(record.source_path)
            try:
                times.append(utc_seconds(record.timestamp))
                kept.append(record)
                for name in VAR_NAMES:
                    var = ds.variables[name]
                    attrs = _extract_var_attrs(var)
                    arr = _read_var_array(var)
                    arr = _replace_sentinels(arr, name, attrs)
                    if arr.shape != (grid_spec.nrows, grid_spec.ncols):
                        raise ValueError(f"{name} shape mismatch: {arr.shape}")
                    per_var[name].append(np.flipud(arr))
            finally:
                ds.close()
        except Exception as exc:
            messages.append(f"{record.source_path.name}: {type(exc).__name__}: {exc}")

    arrays = {
        name: np.stack(items, axis=0).astype(np.float32, copy=False) if items else np.empty((0, grid_spec.nrows, grid_spec.ncols), dtype=np.float32)
        for name, items in per_var.items()
    }
    return BatchPayload(records=kept, times=np.asarray(times, dtype=np.int64), arrays=arrays, messages=messages)


def _prepare_store(out_store: Path, overwrite: bool = False) -> None:
    if out_store.exists():
        if not overwrite:
            return
        if out_store.is_dir():
            shutil.rmtree(out_store)
        else:
            out_store.unlink()
    out_store.parent.mkdir(parents=True, exist_ok=True)


def _json_dump(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True))


def _codec() -> Blosc | None:
    try:
        return Blosc(cname="zstd", clevel=3, shuffle=Blosc.BITSHUFFLE)
    except Exception:
        return None


def _chunk_bytes(array: np.ndarray, codec: Blosc | None) -> bytes:
    block = np.ascontiguousarray(array)
    if codec is None:
        return block.tobytes(order="C")
    return codec.encode(block)


def _write_chunk(path: Path, array: np.ndarray, codec: Blosc | None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(_chunk_bytes(array, codec))


def _array_meta(shape: tuple[int, ...], chunks: tuple[int, ...], dtype: str, compressor: dict[str, Any] | None, fill_value: Any, attrs: dict[str, Any]) -> dict[str, Any]:
    return {
        "zarr_format": 2,
        "shape": list(shape),
        "chunks": list(chunks),
        "dtype": np.dtype(dtype).str,
        "compressor": compressor,
        "fill_value": fill_value,
        "order": "C",
        "filters": None,
        "dimension_separator": ".",
        "attrs": attrs,
    }


def _write_array_metadata(array_dir: Path, meta: dict[str, Any]) -> None:
    array_dir.mkdir(parents=True, exist_ok=True)
    _json_dump(array_dir / ".zarray", {key: value for key, value in meta.items() if key != "attrs"})
    if "attrs" in meta:
        _json_dump(array_dir / ".zattrs", meta["attrs"])


def _write_consolidated_metadata(out_store: Path, root_attrs: dict[str, Any], arrays_meta: dict[str, dict[str, Any]]) -> None:
    metadata: dict[str, Any] = {
        ".zgroup": {"zarr_format": 2},
        ".zattrs": root_attrs,
    }
    for name, meta in arrays_meta.items():
        metadata[f"{name}/.zarray"] = {key: value for key, value in meta.items() if key != "attrs"}
        if "attrs" in meta:
            metadata[f"{name}/.zattrs"] = meta["attrs"]
    _json_dump(out_store / ".zmetadata", {"zarr_consolidated_format": 1, "metadata": metadata})


def _create_spatial_ref_attrs() -> dict[str, Any]:
    crs = CRS.from_epsg(4326)
    return {
        "grid_mapping_name": "latitude_longitude",
        "crs_wkt": crs.to_wkt(),
        "spatial_ref": crs.to_wkt(),
        "epsg_code": "EPSG:4326",
    }


def create_geozarr(
    records: list[ManifestRecord],
    grid_spec: GridSpec,
    out_store: Path,
    *,
    var_attrs: dict[str, dict[str, Any]] | None = None,
    batch_size: int = DEFAULT_BATCH_SIZE,
    workers: int = DEFAULT_WORKERS,
    time_chunk: int = DEFAULT_TIME_CHUNK,
    y_chunk: int = DEFAULT_Y_CHUNK,
    x_chunk: int = DEFAULT_X_CHUNK,
    overwrite: bool = False,
) -> dict[str, Any]:
    _prepare_store(out_store, overwrite=overwrite)
    if out_store.exists() and not overwrite:
        raise FileExistsError(f"{out_store} already exists; pass overwrite=True to rebuild it")

    batch_size = max(1, batch_size)
    time_chunk = max(1, batch_size)
    y_chunk = grid_spec.nrows
    x_chunk = grid_spec.ncols
    batches = chunk_records(records, batch_size)
    valid_count = len(records)
    root_attrs = {
        "title": "IGAD-ICPAC gridded hydrology stack",
        "source": "NetCDF gzip source files",
        "history": "Created by spatial-db gridded GeoZarr pipeline",
        "grid_mapping": "spatial_ref",
        "crs": "EPSG:4326",
        "nrows": grid_spec.nrows,
        "ncols": grid_spec.ncols,
        "xllcorner": grid_spec.xllcorner,
        "yllcorner": grid_spec.yllcorner,
        "xcellsize": grid_spec.cellsize_x,
        "ycellsize": grid_spec.cellsize_y,
    }

    write_offset = 0
    write_messages: list[str] = []
    compressor = _codec()
    data_time_chunk = min(time_chunk, max(1, valid_count))

    with ProcessPoolExecutor(max_workers=max(1, workers)) as pool:
        for payload in pool.map(read_batch, batches, repeat(grid_spec)):
            if payload.messages:
                write_messages.extend(payload.messages)
            count = len(payload.records)
            if count == 0:
                continue

            coords = {
                "time": ("time", payload.times[:count].astype(np.int64, copy=False)),
                "y": ("y", grid_spec.y.astype(np.float64, copy=False)),
                "x": ("x", grid_spec.x.astype(np.float64, copy=False)),
            }
            data_vars = {}
            for name in VAR_NAMES:
                source_attrs = dict((var_attrs or {}).get(name, {}))
                attrs = {
                    "grid_mapping": "spatial_ref",
                    "coordinates": "time y x",
                    "long_name": source_attrs.get("long_name", name.lower() if name != "SM" else "soil_moisture"),
                    "units": source_attrs.get("units", "unknown"),
                }
                if "standard_name" in source_attrs:
                    attrs["standard_name"] = source_attrs["standard_name"]
                data_vars[name] = xr.DataArray(
                    payload.arrays[name][:count].astype(np.float32, copy=False),
                    dims=("time", "y", "x"),
                    attrs=attrs,
                )

            ds = xr.Dataset(data_vars=data_vars, coords=coords, attrs=root_attrs)
            ds["spatial_ref"] = xr.DataArray(0, attrs=_create_spatial_ref_attrs())
            ds["time"].attrs.update(
                {
                    "standard_name": "time",
                    "long_name": "time",
                    "units": "seconds since 1970-01-01 00:00:00",
                    "calendar": "proleptic_gregorian",
                }
            )
            ds["x"].attrs.update(
                {
                    "standard_name": "longitude",
                    "long_name": "x coordinate of projection",
                    "units": "degrees_east",
                }
            )
            ds["y"].attrs.update(
                {
                    "standard_name": "latitude",
                    "long_name": "y coordinate of projection",
                    "units": "degrees_north",
                }
            )

            encoding = {
                "time": {"dtype": "int64", "chunks": (data_time_chunk,)},
                "x": {"dtype": "float64", "chunks": (grid_spec.ncols,)},
                "y": {"dtype": "float64", "chunks": (grid_spec.nrows,)},
                "spatial_ref": {"dtype": "int16"},
            }
            for name in VAR_NAMES:
                encoding[name] = {
                    "dtype": "float32",
                    "chunks": (data_time_chunk, grid_spec.nrows, grid_spec.ncols),
                    "_FillValue": np.nan,
                    "compressor": compressor,
                }

            if write_offset == 0:
                ds.to_zarr(out_store, mode="w", consolidated=False, encoding=encoding, zarr_format=2)
            else:
                ds.to_zarr(out_store, mode="a", append_dim="time", consolidated=False, zarr_format=2)
            ds.close()
            write_offset += count

    valid_count = write_offset
    root_attrs = {**root_attrs, "records_written": valid_count}
    if (out_store / ".zattrs").exists():
        on_disk = json.loads((out_store / ".zattrs").read_text())
        _json_dump(out_store / ".zattrs", {**on_disk, **root_attrs})
    zarr.consolidate_metadata(str(out_store))

    summary = {
        "records_written": valid_count,
        "batch_size": batch_size,
        "workers": workers,
        "time_chunk": time_chunk,
        "y_chunk": y_chunk,
        "x_chunk": x_chunk,
        "grid": {
            "nrows": grid_spec.nrows,
            "ncols": grid_spec.ncols,
            "xllcorner": grid_spec.xllcorner,
            "yllcorner": grid_spec.yllcorner,
            "cellsize_x": grid_spec.cellsize_x,
            "cellsize_y": grid_spec.cellsize_y,
        },
        "messages": write_messages[:100],
    }

    summary_path = out_store.parent / f"{out_store.name}.summary.json"
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True))

    return summary

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build the gridded GeoZarr stack")
    parser.add_argument("--manifest", type=Path, help="Manifest parquet/csv path")
    parser.add_argument("--source-dir", type=Path, default=DEFAULT_SOURCE_DIR, help="Fallback source directory")
    parser.add_argument("--output", type=Path, default=default_output_dir() / DEFAULT_STORE_NAME, help="Output Zarr store")
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS)
    parser.add_argument("--time-chunk", type=int, default=DEFAULT_TIME_CHUNK)
    parser.add_argument("--y-chunk", type=int, default=DEFAULT_Y_CHUNK)
    parser.add_argument("--x-chunk", type=int, default=DEFAULT_X_CHUNK)
    parser.add_argument("--overwrite", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> dict[str, Any]:
    parser = build_parser()
    args = parser.parse_args(argv)

    records = resolve_input_records(manifest=args.manifest, source_dir=args.source_dir)
    candidates = filter_candidate_records(records)
    if not candidates:
        raise FileNotFoundError("No readable gridded source candidates were found")

    readable, grid_spec, var_attrs, probe_results = probe_records(candidates, workers=args.workers)
    bad_count = sum(1 for result in probe_results if not result.ok)

    out_store = args.output
    out_store.parent.mkdir(parents=True, exist_ok=True)

    summary = create_geozarr(
        readable,
        grid_spec,
        out_store,
        var_attrs=var_attrs,
        batch_size=args.batch_size,
        workers=args.workers,
        time_chunk=args.time_chunk,
        y_chunk=args.y_chunk,
        x_chunk=args.x_chunk,
        overwrite=args.overwrite,
    )
    summary["probe_ok"] = len(readable)
    summary["probe_bad"] = bad_count
    return summary


if __name__ == "__main__":
    main()
