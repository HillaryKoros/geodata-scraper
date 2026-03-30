"""Parallel manifest builder for gridded gzipped NetCDF files."""

from __future__ import annotations

import json
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import asdict
from datetime import timedelta
from pathlib import Path
from typing import Iterable

import pandas as pd
from netCDF4 import Dataset
from tqdm import tqdm

from etl.pipelines.gridded.common import (
    DEFAULT_WORKERS,
    MANIFEST_STEM,
    FileInspection,
    EXPECTED_VARIABLES,
    TIMESTAMP_FMT,
    build_record,
    inspect_dataset,
    output_dir,
    parse_timestamp,
    source_dir,
    sync_output_dir,
)

MANIFEST_BASENAME = MANIFEST_STEM
SUMMARY_BASENAME = f"{MANIFEST_STEM}_summary"


def _inspect_one(path: Path) -> FileInspection:
    """Inspect one gzipped NetCDF file and classify any failure."""
    size_bytes = path.stat().st_size if path.exists() else 0
    ts = parse_timestamp(path)
    if size_bytes == 0:
        return build_record(
            path,
            "zero_byte",
            error_type="empty_file",
            error_message="File size is zero bytes",
        )

    if ts is None:
        name_status = "invalid_name"
    else:
        name_status = "ok"

    try:
        from etl.pipelines.gridded.common import load_payload

        payload = load_payload(path)
    except Exception as exc:  # gzip corruption or truncated stream
        return build_record(
            path,
            "gzip_error",
            error_type=type(exc).__name__,
            error_message=str(exc),
            gzip_ok=False,
            netcdf_ok=False,
        )

    try:
        with Dataset("inmemory.nc", memory=payload) as ds:
            metadata = inspect_dataset(ds)
    except Exception as exc:
        return build_record(
            path,
            "netcdf_error",
            error_type=type(exc).__name__,
            error_message=str(exc),
            gzip_ok=True,
            netcdf_ok=False,
        )

    status = name_status
    missing = metadata.get("missing_variables", [])
    if missing:
        status = "missing_expected_variables"

    return build_record(
        path,
        status,
        gzip_ok=True,
        netcdf_ok=True,
        metadata=metadata,
    )


def _iter_source_files(root: Path, limit: int | None = None) -> list[Path]:
    files = sorted(f for f in root.glob("*.nc.gz") if f.is_file())
    if limit is not None:
        files = files[:limit]
    return files


def _record_dict(record: FileInspection) -> dict:
    return asdict(record)


def _missing_slots(timestamps: Iterable[str]) -> list[str]:
    values = sorted({ts for ts in timestamps if ts})
    if not values:
        return []

    parsed = pd.Series(pd.to_datetime(values, format=TIMESTAMP_FMT, errors="coerce")).dropna().sort_values()
    if parsed.empty:
        return []

    present = {ts.strftime(TIMESTAMP_FMT) for ts in parsed}
    missing = []
    current = parsed.iloc[0].to_pydatetime()
    end = parsed.iloc[-1].to_pydatetime()
    while current <= end:
        key = current.strftime(TIMESTAMP_FMT)
        if key not in present:
            missing.append(key)
        current += timedelta(hours=6)
    return missing


def _summary(records: list[FileInspection], source_root: Path, out_dir: Path) -> dict:
    df = pd.DataFrame([_record_dict(r) for r in records])
    status_counts = df["status"].value_counts(dropna=False).to_dict()
    error_counts = (
        df.loc[df["error_type"].notna(), "error_type"].value_counts(dropna=False).to_dict()
        if not df.empty
        else {}
    )
    timestamps = [ts for ts in df.get("timestamp", []) if isinstance(ts, str) and ts]
    missing = _missing_slots(timestamps)
    ok_df = df[df["status"] == "ok"] if not df.empty else df
    sample = ok_df.iloc[0].to_dict() if not ok_df.empty else {}

    grid = {}
    variables = {}
    if sample:
        try:
            grid = json.loads(sample.get("grid_json") or "{}")
        except Exception:
            grid = {}
        try:
            variables = json.loads(sample.get("variables_json") or "{}")
        except Exception:
            variables = {}

    return {
        "source_dir": str(source_root),
        "output_dir": str(out_dir),
        "expected_variables": list(EXPECTED_VARIABLES),
        "generated_at": pd.Timestamp.utcnow().isoformat(),
        "total_files": int(len(df)),
        "ok_files": int((df["status"] == "ok").sum()) if not df.empty else 0,
        "invalid_files": int((df["status"] != "ok").sum()) if not df.empty else 0,
        "status_counts": status_counts,
        "error_counts": error_counts,
        "first_timestamp": df["timestamp"].dropna().min() if not df.empty else None,
        "last_timestamp": df["timestamp"].dropna().max() if not df.empty else None,
        "missing_6h_slots": len(missing),
        "missing_6h_slot_examples": missing[:25],
        "sample_grid": grid,
        "sample_variables": variables,
        "sample_file": sample.get("file_name"),
    }


def build_manifest(
    source_root: Path | None = None,
    *,
    workers: int = DEFAULT_WORKERS,
    limit: int | None = None,
    output_subdir: str | None = None,
) -> tuple[pd.DataFrame, dict]:
    """Scan gridded source files and write a structured manifest."""
    root = Path(source_root or source_dir())
    if not root.exists():
        raise FileNotFoundError(f"Source directory does not exist: {root}")

    out_dir = output_dir(output_subdir)
    out_dir.mkdir(parents=True, exist_ok=True)

    files = _iter_source_files(root, limit=limit)
    if not files:
        raise FileNotFoundError(f"No .nc.gz files found in {root}")

    records: list[FileInspection] = []
    with ProcessPoolExecutor(max_workers=max(1, workers)) as pool:
        futures = {pool.submit(_inspect_one, path): path for path in files}
        with tqdm(total=len(futures), desc="Gridded manifest", unit="file") as pbar:
            for fut in as_completed(futures):
                records.append(fut.result())
                pbar.update(1)

    records.sort(key=lambda r: (r.timestamp or "", r.file_name))
    df = pd.DataFrame([_record_dict(r) for r in records])

    parquet_path = out_dir / f"{MANIFEST_BASENAME}.parquet"
    csv_path = out_dir / f"{MANIFEST_BASENAME}.csv"
    summary_path = out_dir / f"{SUMMARY_BASENAME}.json"

    df.to_parquet(parquet_path, index=False, engine="pyarrow")
    df.to_csv(csv_path, index=False)

    summary = _summary(records, root, out_dir)
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True))

    try:
        sync_output_dir(output_subdir)
    except Exception:
        pass
    return df, summary
