"""Pre-processing validation for gridded source NetCDF files.

Goes deeper than the manifest step: opens each file, reads actual data
arrays, and checks for data quality issues before committing to the
expensive Zarr stacking step.

Checks per file:
  - Gzip + NetCDF readability (same as manifest)
  - Grid shape consistency across all files
  - Physical range checks on SM, Discharge, ET
  - Sentinel / fill-value detection
  - All-NaN or all-zero slice detection
  - Timestamp ordering and gap detection
"""

from __future__ import annotations

import argparse
import json
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import numpy as np
from tqdm import tqdm

from etl.pipelines.gridded.common import (
    DEFAULT_WORKERS,
    EXPECTED_VARIABLES,
    TIMESTAMP_FMT,
    load_payload,
    output_dir,
    parse_timestamp,
    source_dir,
)

# Physical bounds — values outside these are flagged
PHYSICAL_BOUNDS: dict[str, tuple[float, float]] = {
    "SM": (0.0, 1.0),
    "Discharge": (0.0, 1e6),
    "ET": (-10.0, 100.0),
}

# Known sentinel / fill values to detect
SENTINELS = [-9999.0, -32768.0, -8999999815811072.0, -9.999e15, 1e20]


@dataclass
class FileQA:
    """Quality assessment for one source file."""

    file_name: str
    timestamp: str | None
    status: str  # ok | warning | error
    grid_shape: tuple[int, int] | None = None
    issues: list[str] = field(default_factory=list)
    var_stats: dict[str, dict[str, Any]] = field(default_factory=dict)


@dataclass
class InputReport:
    """Aggregate input validation report."""

    source_dir: str
    total_files: int = 0
    ok_files: int = 0
    warning_files: int = 0
    error_files: int = 0
    grid_shapes: dict[str, int] = field(default_factory=dict)
    timestamp_range: tuple[str, str] | None = None
    missing_slots: list[str] = field(default_factory=list)
    file_results: list[FileQA] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        error_files = [f for f in self.file_results if f.status == "error"]
        warning_files = [f for f in self.file_results if f.status == "warning"]
        return {
            "source_dir": self.source_dir,
            "total_files": self.total_files,
            "ok_files": self.ok_files,
            "warning_files": self.warning_files,
            "error_files": self.error_files,
            "grid_shapes": self.grid_shapes,
            "dominant_shape": max(self.grid_shapes, key=self.grid_shapes.get) if self.grid_shapes else None,
            "timestamp_range": self.timestamp_range,
            "missing_6h_slots": len(self.missing_slots),
            "missing_6h_slot_examples": self.missing_slots[:25],
            "error_file_examples": [
                {"file": f.file_name, "issues": f.issues} for f in error_files[:20]
            ],
            "warning_file_examples": [
                {"file": f.file_name, "issues": f.issues} for f in warning_files[:20]
            ],
        }


def _inspect_one(path: Path) -> FileQA:
    """Deep-inspect one gzipped NetCDF source file."""
    from netCDF4 import Dataset

    ts = parse_timestamp(path)
    ts_key = ts.strftime(TIMESTAMP_FMT) if ts else None
    issues: list[str] = []
    var_stats: dict[str, dict[str, Any]] = {}
    grid_shape: tuple[int, int] | None = None

    # 1. Gzip readability
    try:
        payload = load_payload(path)
    except Exception as exc:
        return FileQA(
            file_name=path.name,
            timestamp=ts_key,
            status="error",
            issues=[f"gzip error: {exc}"],
        )

    # 2. NetCDF readability
    try:
        ds = Dataset("inmemory.nc", memory=payload)
    except Exception as exc:
        return FileQA(
            file_name=path.name,
            timestamp=ts_key,
            status="error",
            issues=[f"netcdf error: {exc}"],
        )

    try:
        # 3. Grid shape
        nrows = len(ds.dimensions.get("south_north", []))
        ncols = len(ds.dimensions.get("west_east", []))
        if nrows > 0 and ncols > 0:
            grid_shape = (nrows, ncols)
        else:
            issues.append(f"unexpected grid dims: south_north={nrows}, west_east={ncols}")

        # 4. Variable checks
        for var_name in EXPECTED_VARIABLES:
            if var_name not in ds.variables:
                issues.append(f"missing variable: {var_name}")
                continue

            var = ds.variables[var_name]
            raw = var[:]
            if np.ma.isMaskedArray(raw):
                arr = raw.filled(np.nan).astype(np.float32)
            else:
                arr = np.asarray(raw, dtype=np.float32)

            total = arr.size
            nan_count = int(np.isnan(arr).sum())
            inf_count = int(np.isinf(arr).sum())
            finite = arr[np.isfinite(arr)]

            stats: dict[str, Any] = {
                "shape": list(arr.shape),
                "nan_pct": round(nan_count / total * 100, 1) if total > 0 else 0,
                "inf_count": inf_count,
            }

            if len(finite) > 0:
                vmin = float(np.min(finite))
                vmax = float(np.max(finite))
                stats["min"] = vmin
                stats["max"] = vmax
                stats["mean"] = float(np.mean(finite))

                # Sentinel detection (informational — Zarr step replaces these)
                sentinel_total = 0
                for sentinel in SENTINELS:
                    count = int(np.isclose(finite, sentinel).sum())
                    if count > 0:
                        sentinel_total += count
                        stats[f"sentinel_{sentinel}"] = count
                if sentinel_total > 0:
                    stats["sentinel_count"] = sentinel_total
                    stats["sentinel_pct"] = round(sentinel_total / total * 100, 1)

                # Physical bounds — check after masking sentinels
                clean = finite[~np.isclose(finite, -9999.0) & ~np.isclose(finite, -32768.0)]
                if len(clean) > 0:
                    cmin = float(np.min(clean))
                    cmax = float(np.max(clean))
                    lo, hi = PHYSICAL_BOUNDS.get(var_name, (-np.inf, np.inf))
                    if cmin < lo or cmax > hi:
                        issues.append(f"{var_name}: clean range [{cmin:.4f}, {cmax:.4f}] outside [{lo}, {hi}]")

                # All-zero check
                zero_pct = float((finite == 0).sum()) / len(finite) * 100
                stats["zero_pct"] = round(zero_pct, 1)
            else:
                issues.append(f"{var_name}: all values are NaN")
                stats["all_nan"] = True

            if inf_count > 0:
                issues.append(f"{var_name}: {inf_count} Inf values")

            var_stats[var_name] = stats

    finally:
        ds.close()

    status = "ok"
    if any("error" in i.lower() or "missing variable" in i for i in issues):
        status = "error"
    elif issues:
        status = "warning"

    return FileQA(
        file_name=path.name,
        timestamp=ts_key,
        status=status,
        grid_shape=grid_shape,
        issues=issues,
        var_stats=var_stats,
    )


def _find_missing_slots(timestamps: list[str]) -> list[str]:
    """Find missing 6-hourly slots in sorted timestamp list."""
    if len(timestamps) < 2:
        return []
    present = set(timestamps)
    start = datetime.strptime(timestamps[0], TIMESTAMP_FMT)
    end = datetime.strptime(timestamps[-1], TIMESTAMP_FMT)
    missing = []
    current = start
    while current <= end:
        key = current.strftime(TIMESTAMP_FMT)
        if key not in present:
            missing.append(key)
        current += timedelta(hours=6)
    return missing


def validate_inputs(
    source_root: Path | None = None,
    *,
    workers: int = DEFAULT_WORKERS,
    limit: int | None = None,
    sample: int | None = None,
) -> InputReport:
    """Validate source files and return a report."""
    root = Path(source_root or source_dir())
    if not root.exists():
        raise FileNotFoundError(f"Source directory does not exist: {root}")

    files = sorted(f for f in root.glob("*.nc.gz") if f.is_file())
    if not files:
        raise FileNotFoundError(f"No .nc.gz files found in {root}")

    if limit is not None:
        files = files[:limit]

    if sample is not None and sample < len(files):
        rng = np.random.default_rng(42)
        indices = np.sort(rng.choice(len(files), size=sample, replace=False))
        files = [files[i] for i in indices]

    report = InputReport(source_dir=str(root), total_files=len(files))
    results: list[FileQA] = []

    with ProcessPoolExecutor(max_workers=max(1, workers)) as pool:
        futures = {pool.submit(_inspect_one, path): path for path in files}
        with tqdm(total=len(futures), desc="Validating inputs", unit="file") as pbar:
            for fut in as_completed(futures):
                results.append(fut.result())
                pbar.update(1)

    results.sort(key=lambda r: (r.timestamp or "", r.file_name))
    report.file_results = results

    # Aggregate
    for r in results:
        if r.status == "ok":
            report.ok_files += 1
        elif r.status == "warning":
            report.warning_files += 1
        else:
            report.error_files += 1

        if r.grid_shape is not None:
            key = f"{r.grid_shape[0]}x{r.grid_shape[1]}"
            report.grid_shapes[key] = report.grid_shapes.get(key, 0) + 1

    timestamps = sorted([r.timestamp for r in results if r.timestamp])
    if timestamps:
        report.timestamp_range = (timestamps[0], timestamps[-1])
        report.missing_slots = _find_missing_slots(timestamps)

    return report


def main(argv: list[str] | None = None) -> None:
    """Run input validation and print results."""
    parser = argparse.ArgumentParser(description="Validate gridded source NetCDF files")
    parser.add_argument("--source-dir", type=Path, default=None)
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS)
    parser.add_argument("--limit", type=int, default=None, help="Process first N files only")
    parser.add_argument("--sample", type=int, default=None, help="Random sample of N files")
    parser.add_argument("--json", action="store_true", help="Output as JSON")
    parser.add_argument("--save", action="store_true", help="Save report to output dir")
    args = parser.parse_args(argv)

    report = validate_inputs(
        source_root=args.source_dir,
        workers=args.workers,
        limit=args.limit,
        sample=args.sample,
    )

    summary = report.to_dict()

    if args.json:
        print(json.dumps(summary, indent=2))
    else:
        print(f"\nInput Validation")
        print(f"  Source: {report.source_dir}")
        print(f"  Files:  {report.total_files} total")
        print(f"  OK:     {report.ok_files}")
        print(f"  Warn:   {report.warning_files}")
        print(f"  Error:  {report.error_files}")
        print(f"  Shapes: {report.grid_shapes}")
        if report.timestamp_range:
            print(f"  Time:   {report.timestamp_range[0]} -> {report.timestamp_range[1]}")
        print(f"  Missing 6h slots: {len(report.missing_slots)}")
        if report.missing_slots[:5]:
            print(f"  Examples: {report.missing_slots[:5]}")

        if report.error_files > 0:
            print(f"\n  Error files ({report.error_files}):")
            for r in report.file_results:
                if r.status == "error":
                    print(f"    {r.file_name}: {'; '.join(r.issues)}")

        if report.warning_files > 0:
            print(f"\n  Warning files ({report.warning_files}):")
            for r in report.file_results[:20]:
                if r.status == "warning":
                    print(f"    {r.file_name}: {'; '.join(r.issues)}")

    if args.save:
        out = output_dir()
        out.mkdir(parents=True, exist_ok=True)
        report_path = out / "input_validation_report.json"
        report_path.write_text(json.dumps(summary, indent=2))
        print(f"\nReport saved to {report_path}")

    if report.error_files > 0:
        error_pct = report.error_files / report.total_files * 100
        if error_pct > 10:
            print(f"\nFATAL: {error_pct:.1f}% of files have errors — aborting")
            raise SystemExit(1)
        else:
            print(f"\nWARNING: {report.error_files} files ({error_pct:.1f}%) have errors — Zarr step will skip them")


if __name__ == "__main__":
    main()
