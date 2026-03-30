"""Optional COG export helpers for the gridded stack.

COGs can be produced either from the GeoZarr store or directly from validated
source files referenced by the manifest.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import importlib.util
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import rasterio
from numcodecs import get_codec
from rasterio.crs import CRS
from rasterio.transform import Affine

from etl.pipelines.gridded.zarr import (
    DEFAULT_SOURCE_DIR,
    DEFAULT_WORKERS,
    VAR_NAMES,
    GridSpec,
    ManifestRecord,
    _extract_var_attrs,
    _open_dataset_from_gz,
    _replace_sentinels,
    default_output_dir,
    filter_candidate_records,
    parse_timestamp,
    probe_records,
    resolve_input_records,
)

_CORE_CONFIG_PATH = Path(__file__).resolve().parents[2] / "core" / "config.py"
_CORE_CONFIG_SPEC = importlib.util.spec_from_file_location("spatial_db_core_config", _CORE_CONFIG_PATH)
if _CORE_CONFIG_SPEC is None or _CORE_CONFIG_SPEC.loader is None:
    raise ImportError(f"Cannot load core config from {_CORE_CONFIG_PATH}")
_CORE_CONFIG = importlib.util.module_from_spec(_CORE_CONFIG_SPEC)
_CORE_CONFIG_SPEC.loader.exec_module(_CORE_CONFIG)
sync_to_ssd = _CORE_CONFIG.sync_to_ssd

DEFAULT_COG_DIR = os.getenv("GRIDDED_COG_DIR")
DEFAULT_COG_LIMIT = int(os.getenv("GRIDDED_COG_LIMIT", "0"))


@dataclass(frozen=True)
class CogSpec:
    variable: str
    timestamp: datetime
    source: Path
    output: Path


def _cog_root(output_dir: Path | None = None) -> Path:
    if output_dir is not None:
        return output_dir
    if DEFAULT_COG_DIR:
        return Path(DEFAULT_COG_DIR)
    return default_output_dir() / "cogs"


def _coerce_records(manifest: Path | None = None, source_dir: Path | None = None) -> tuple[list[ManifestRecord], GridSpec]:
    records = resolve_input_records(manifest=manifest, source_dir=source_dir)
    candidates = filter_candidate_records(records)
    readable, grid_spec, _, _ = probe_records(candidates, workers=DEFAULT_WORKERS)
    return readable, grid_spec


def _slice_from_source(record: ManifestRecord, variable: str, grid_spec: GridSpec) -> tuple[np.ndarray, datetime]:
    ds = _open_dataset_from_gz(record.source_path)
    try:
        var = ds.variables[variable]
        attrs = _extract_var_attrs(var)
        arr = np.asarray(var[:], dtype=np.float32)
        if np.ma.isMaskedArray(arr):
            arr = arr.filled(np.nan)
        arr = np.array(arr, dtype=np.float32, copy=True)
        arr = _replace_sentinels(arr, variable, attrs)
        if arr.shape != (grid_spec.nrows, grid_spec.ncols):
            raise ValueError(f"{variable} shape mismatch: {arr.shape}")
        return np.flipud(arr), record.timestamp
    finally:
        ds.close()


def _write_cog_raster(array: np.ndarray, out_path: Path, spec: GridSpec, *, nodata: float = np.nan) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    profile = {
        "driver": "COG",
        "height": array.shape[0],
        "width": array.shape[1],
        "count": 1,
        "dtype": "float32",
        "crs": spec.crs,
        "transform": spec.transform,
        "nodata": nodata,
        "compress": "DEFLATE",
        "blockxsize": 256,
        "blockysize": 256,
    }
    try:
        with rasterio.open(out_path, "w", **profile) as dst:
            dst.write(array.astype(np.float32, copy=False), 1)
        return
    except Exception:
        pass

    tmp_tif = out_path.parent / f".{out_path.stem}.tmp.tif"
    try:
        with rasterio.open(
            tmp_tif,
            "w",
            driver="GTiff",
            height=array.shape[0],
            width=array.shape[1],
            count=1,
            dtype="float32",
            crs=spec.crs,
            transform=spec.transform,
            nodata=nodata,
            compress="DEFLATE",
        ) as dst:
            dst.write(array.astype(np.float32, copy=False), 1)
        subprocess.run(
            [
                "gdal_translate",
                "-of",
                "COG",
                "-co",
                "COMPRESS=DEFLATE",
                "-co",
                "BLOCKSIZE=256",
                str(tmp_tif),
                str(out_path),
            ],
            check=True,
            capture_output=True,
        )
    finally:
        tmp_tif.unlink(missing_ok=True)


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text())


def _codec_from_meta(meta: dict[str, Any]):
    compressor = meta.get("compressor")
    if compressor is None:
        return None
    return get_codec(compressor)


def _decode_chunk(path: Path, meta: dict[str, Any]) -> np.ndarray:
    codec = _codec_from_meta(meta)
    payload = path.read_bytes()
    raw = codec.decode(payload) if codec is not None else payload
    dtype = np.dtype(meta["dtype"])
    return np.frombuffer(raw, dtype=dtype)


def _load_zarr_store(store: Path) -> tuple[dict[str, Any], dict[str, dict[str, Any]]]:
    root_attrs = _read_json(store / ".zattrs") if (store / ".zattrs").exists() else {}
    arrays: dict[str, dict[str, Any]] = {}
    for name in ["time", "x", "y", "spatial_ref", *VAR_NAMES]:
        meta_path = store / name / ".zarray"
        attrs_path = store / name / ".zattrs"
        if not meta_path.exists():
            continue
        arrays[name] = {
            "meta": _read_json(meta_path),
            "attrs": _read_json(attrs_path) if attrs_path.exists() else {},
        }
    return root_attrs, arrays


def _grid_spec_from_store(root_attrs: dict[str, Any], arrays: dict[str, dict[str, Any]], store: Path) -> GridSpec:
    x_meta = arrays["x"]["meta"]
    y_meta = arrays["y"]["meta"]
    x = _decode_chunk(store / "x" / "0", x_meta).astype(np.float64, copy=False)
    y = _decode_chunk(store / "y" / "0", y_meta).astype(np.float64, copy=False)
    dx = float(root_attrs.get("xcellsize", x[1] - x[0] if len(x) > 1 else 1.0))
    dy = float(root_attrs.get("ycellsize", abs(y[1] - y[0]) if len(y) > 1 else 1.0))
    xll = float(root_attrs.get("xllcorner", x[0] - dx / 2.0 if len(x) else 0.0))
    yll = float(root_attrs.get("yllcorner", y[-1] - dy / 2.0 if len(y) else 0.0))
    north = yll + len(y) * dy
    return GridSpec(
        nrows=len(y),
        ncols=len(x),
        xllcorner=xll,
        yllcorner=yll,
        cellsize_x=dx,
        cellsize_y=dy,
        transform=Affine(dx, 0.0, xll, 0.0, -dy, north),
        crs=CRS.from_epsg(4326),
        x=x,
        y=y,
    )


def _load_time_values(store: Path, meta: dict[str, Any]) -> np.ndarray:
    total = int(meta["shape"][0])
    chunk = int(meta["chunks"][0])
    values: list[np.ndarray] = []
    for chunk_index, start in enumerate(range(0, total, chunk)):
        chunk_path = store / "time" / str(chunk_index)
        if not chunk_path.exists():
            break
        arr = _decode_chunk(chunk_path, meta).astype(np.int64, copy=False)
        values.append(arr.reshape(-1))
    if not values:
        return np.empty((0,), dtype=np.int64)
    return np.concatenate(values)[:total]


def _load_time_slice(store: Path, meta: dict[str, Any], index: int) -> int:
    chunk = int(meta["chunks"][0])
    chunk_index = index // chunk
    within = index % chunk
    chunk_path = store / "time" / str(chunk_index)
    arr = _decode_chunk(chunk_path, meta).astype(np.int64, copy=False).reshape(-1)
    return int(arr[within])


def _load_var_slice(store: Path, meta: dict[str, Any], index: int) -> np.ndarray:
    chunk = int(meta["chunks"][0])
    chunk_index = index // chunk
    within = index % chunk
    # Variable chunk files are stored as <batch>.0.0 under the variable directory.
    chunk_path = store / meta["name"] / f"{chunk_index}.0.0"
    arr = _decode_chunk(chunk_path, meta).astype(np.float32, copy=False)
    nrows = int(meta["shape"][1])
    ncols = int(meta["shape"][2])
    return arr.reshape((-1, nrows, ncols))[within]


def export_from_zarr(store: Path, out_dir: Path, *, variables: Iterable[str] = VAR_NAMES, limit: int = 0) -> list[Path]:
    root_attrs, arrays = _load_zarr_store(store)
    if "time" not in arrays or "x" not in arrays or "y" not in arrays:
        raise FileNotFoundError(f"{store} does not look like a gridded Zarr store")

    spec = _grid_spec_from_store(root_attrs, arrays, store)
    total = int(arrays["time"]["meta"]["shape"][0])
    if limit > 0:
        total = min(total, limit)
    names = [name for name in variables if name in arrays]
    exported: list[Path] = []
    out_dir.mkdir(parents=True, exist_ok=True)

    for index in range(total):
        ts_seconds = _load_time_slice(store, arrays["time"]["meta"], index)
        timestamp = datetime.fromtimestamp(ts_seconds, tz=timezone.utc)
        for name in names:
            var_meta = dict(arrays[name]["meta"])
            var_meta["name"] = name
            array = _load_var_slice(store, var_meta, index)
            out_path = out_dir / f"{name}_{timestamp:%Y%m%d%H%M}.tif"
            _write_cog_raster(array, out_path, spec)
            exported.append(out_path)
    return exported


def export_from_source(records: list[ManifestRecord], grid_spec: GridSpec, out_dir: Path, *, variables: Iterable[str] = VAR_NAMES, limit: int = 0) -> list[Path]:
    out_dir.mkdir(parents=True, exist_ok=True)
    exported: list[Path] = []
    selected = [record for record in records if record.source_path.exists()]
    if limit > 0:
        selected = selected[:limit]
    for record in selected:
        for name in variables:
            array, timestamp = _slice_from_source(record, name, grid_spec)
            out_path = out_dir / f"{name}_{timestamp:%Y%m%d%H%M}.tif"
            _write_cog_raster(array, out_path, grid_spec)
            exported.append(out_path)
    return exported


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Export per-timestep COGs from the gridded stack")
    parser.add_argument("--manifest", type=Path)
    parser.add_argument("--source-dir", type=Path, default=DEFAULT_SOURCE_DIR)
    parser.add_argument("--store", type=Path, default=default_output_dir() / os.getenv("GRIDDED_ZARR_STORE", "gridded_geozarr.zarr"))
    parser.add_argument("--output-dir", type=Path, default=_cog_root())
    parser.add_argument("--variables", default=",".join(VAR_NAMES))
    parser.add_argument("--limit", type=int, default=DEFAULT_COG_LIMIT)
    parser.add_argument("--source-mode", choices=("zarr", "source"), default="zarr")
    parser.add_argument("--overwrite", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> dict[str, Any]:
    parser = build_parser()
    args = parser.parse_args(argv)
    variables = [name.strip() for name in args.variables.split(",") if name.strip()]
    out_dir = args.output_dir
    if args.overwrite and out_dir.exists():
        shutil.rmtree(out_dir)

    if args.source_mode == "zarr" and args.store.exists():
        exported = export_from_zarr(args.store, out_dir, variables=variables, limit=args.limit)
    else:
        records, grid_spec = _coerce_records(manifest=args.manifest, source_dir=args.source_dir)
        exported = export_from_source(records, grid_spec, out_dir, variables=variables, limit=args.limit)

    summary = {
        "exported": len(exported),
        "output_dir": str(out_dir),
        "variables": variables,
        "mode": args.source_mode,
        "limit": args.limit,
    }
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "cogs.summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True))
    try:
        sync_to_ssd(f"processed/{getattr(_CORE_CONFIG, 'GRIDDED_PROCESSED_SUBDIR', 'gridded')}")
    except Exception:
        pass
    return summary


if __name__ == "__main__":
    main()
