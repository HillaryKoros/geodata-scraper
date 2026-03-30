"""Post-processing validation for gridded Zarr store and COG outputs.

Checks the Zarr store is readable, variables have sensible ranges,
coordinates are consistent, and COGs (if present) are valid GeoTIFFs.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import xarray as xr

from etl.pipelines.gridded.common import (
    DEFAULT_WORKERS,
    EXPECTED_VARIABLES,
    ZARR_STORE_NAME,
    COG_SUBDIR,
    output_dir,
)


# Physical bounds per variable — values outside these are suspect
PHYSICAL_BOUNDS: dict[str, tuple[float, float]] = {
    "SM": (0.0, 1.0),
    "Discharge": (0.0, 1e6),
    "ET": (-10.0, 100.0),
}

# Maximum acceptable NaN fraction per variable
MAX_NAN_FRACTION: dict[str, float] = {
    "SM": 0.80,
    "Discharge": 0.80,
    "ET": 0.80,
}


@dataclass
class Check:
    """One validation check result."""

    name: str
    passed: bool
    message: str
    severity: str = "error"  # error | warning


@dataclass
class ValidationReport:
    """Aggregate validation report."""

    zarr_store: str
    cog_dir: str | None
    timestamp: str = ""
    checks: list[Check] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return all(c.passed for c in self.checks if c.severity == "error")

    @property
    def errors(self) -> list[Check]:
        return [c for c in self.checks if not c.passed and c.severity == "error"]

    @property
    def warnings(self) -> list[Check]:
        return [c for c in self.checks if not c.passed and c.severity == "warning"]

    def to_dict(self) -> dict[str, Any]:
        return {
            "passed": self.passed,
            "zarr_store": self.zarr_store,
            "cog_dir": self.cog_dir,
            "timestamp": self.timestamp,
            "total_checks": len(self.checks),
            "errors": len(self.errors),
            "warnings": len(self.warnings),
            "checks": [
                {"name": c.name, "passed": c.passed, "severity": c.severity, "message": c.message}
                for c in self.checks
            ],
        }


def _check_zarr_readable(store: Path) -> Check:
    """Can xarray open the store?"""
    try:
        ds = xr.open_zarr(store)
        ds.close()
        return Check("zarr_readable", True, f"Zarr store opens successfully: {store}")
    except Exception as exc:
        return Check("zarr_readable", False, f"Cannot open Zarr store: {exc}")


def _check_expected_variables(ds: xr.Dataset) -> list[Check]:
    checks = []
    present = set(ds.data_vars)
    for var in EXPECTED_VARIABLES:
        if var in present:
            checks.append(Check(f"var_present_{var}", True, f"{var} found in store"))
        else:
            checks.append(Check(f"var_present_{var}", False, f"{var} missing from store"))
    return checks


def _check_dimensions(ds: xr.Dataset) -> list[Check]:
    checks = []
    for dim in ("time", "y", "x"):
        if dim in ds.dims:
            size = ds.sizes[dim]
            checks.append(Check(f"dim_{dim}", True, f"Dimension '{dim}' present (size={size})"))
        else:
            checks.append(Check(f"dim_{dim}", False, f"Dimension '{dim}' missing"))
    if "time" in ds.dims and ds.sizes["time"] == 0:
        checks.append(Check("dim_time_nonempty", False, "Time dimension is empty"))
    elif "time" in ds.dims:
        checks.append(Check("dim_time_nonempty", True, f"Time dimension has {ds.sizes['time']} steps"))
    return checks


def _check_coordinates(ds: xr.Dataset) -> list[Check]:
    checks = []
    if "y" in ds.coords:
        y = ds.y.values
        if len(y) > 1:
            diffs = np.diff(y)
            monotonic = bool(np.all(diffs > 0) or np.all(diffs < 0))
            direction = "descending (north-up)" if np.all(diffs < 0) else "ascending"
            checks.append(
                Check("y_monotonic", monotonic, f"y coordinate is {direction}, dy={float(np.mean(diffs)):.6f}")
            )
        if np.any(np.isnan(y)):
            checks.append(Check("y_no_nan", False, "y coordinate contains NaN values"))
        else:
            checks.append(Check("y_no_nan", True, f"y range: [{y.min():.4f}, {y.max():.4f}]"))

    if "x" in ds.coords:
        x = ds.x.values
        if len(x) > 1:
            diffs = np.diff(x)
            monotonic = bool(np.all(diffs > 0))
            checks.append(
                Check("x_monotonic", monotonic, f"x coordinate monotonic increasing, dx={float(np.mean(diffs)):.6f}")
            )
        if np.any(np.isnan(x)):
            checks.append(Check("x_no_nan", False, "x coordinate contains NaN values"))
        else:
            checks.append(Check("x_no_nan", True, f"x range: [{x.min():.4f}, {x.max():.4f}]"))

    if "time" in ds.coords:
        times = ds.time.values
        if len(times) > 1:
            sorted_ok = bool(np.all(np.diff(times.astype(np.int64)) >= 0))
            checks.append(Check("time_sorted", sorted_ok, f"Time coordinate sorted: {len(times)} steps"))
            dupes = len(times) - len(np.unique(times))
            if dupes > 0:
                checks.append(Check("time_unique", False, f"{dupes} duplicate timestamps found", severity="warning"))
            else:
                checks.append(Check("time_unique", True, "All timestamps unique"))
    return checks


def _check_crs(ds: xr.Dataset) -> list[Check]:
    checks = []
    crs_attr = ds.attrs.get("crs", "")
    if "4326" in str(crs_attr):
        checks.append(Check("crs_epsg4326", True, f"CRS is {crs_attr}"))
    else:
        checks.append(Check("crs_epsg4326", False, f"Expected EPSG:4326, got '{crs_attr}'", severity="warning"))

    if "spatial_ref" in ds:
        checks.append(Check("spatial_ref_present", True, "spatial_ref variable present"))
    else:
        checks.append(Check("spatial_ref_present", False, "spatial_ref variable missing", severity="warning"))
    return checks


def _check_variable_ranges(ds: xr.Dataset) -> list[Check]:
    """Check data ranges and NaN fractions for each expected variable."""
    checks = []
    for var in EXPECTED_VARIABLES:
        if var not in ds.data_vars:
            continue
        data = ds[var].values
        total = data.size
        nan_count = int(np.isnan(data).sum())
        nan_frac = nan_count / total if total > 0 else 0.0

        max_nan = MAX_NAN_FRACTION.get(var, 0.80)
        if nan_frac > max_nan:
            checks.append(
                Check(f"nan_fraction_{var}", False, f"{var} NaN fraction {nan_frac:.1%} exceeds {max_nan:.0%}")
            )
        else:
            checks.append(
                Check(f"nan_fraction_{var}", True, f"{var} NaN fraction {nan_frac:.1%} (limit {max_nan:.0%})")
            )

        if nan_count < total:
            vmin = float(np.nanmin(data))
            vmax = float(np.nanmax(data))
            lo, hi = PHYSICAL_BOUNDS.get(var, (-np.inf, np.inf))
            in_bounds = vmin >= lo and vmax <= hi
            checks.append(
                Check(
                    f"range_{var}",
                    in_bounds,
                    f"{var} range [{vmin:.4f}, {vmax:.4f}] vs bounds [{lo}, {hi}]",
                    severity="warning" if not in_bounds else "error",
                )
            )

            inf_count = int(np.isinf(data).sum())
            if inf_count > 0:
                checks.append(Check(f"no_inf_{var}", False, f"{var} has {inf_count} Inf values"))
            else:
                checks.append(Check(f"no_inf_{var}", True, f"{var} has no Inf values"))
    return checks


def _check_cogs(cog_dir: Path) -> list[Check]:
    """Validate COG files if they exist."""
    checks = []
    if not cog_dir.exists():
        checks.append(Check("cog_dir_exists", True, "No COG directory (skipped)", severity="warning"))
        return checks

    tifs = sorted(cog_dir.glob("*.tif"))
    if not tifs:
        checks.append(Check("cog_files_exist", False, f"COG directory exists but no .tif files: {cog_dir}", severity="warning"))
        return checks

    checks.append(Check("cog_files_exist", True, f"{len(tifs)} COG files found"))

    try:
        import rasterio
    except ImportError:
        checks.append(Check("cog_rasterio", False, "rasterio not available — cannot validate COGs", severity="warning"))
        return checks

    errors = 0
    for tif in tifs[:10]:  # spot-check first 10
        try:
            with rasterio.open(tif) as src:
                if src.crs is None:
                    errors += 1
                    checks.append(Check(f"cog_crs_{tif.name}", False, f"{tif.name}: no CRS"))
                elif src.crs.to_epsg() != 4326:
                    checks.append(
                        Check(f"cog_crs_{tif.name}", False, f"{tif.name}: CRS={src.crs}, expected EPSG:4326", severity="warning")
                    )
                data = src.read(1)
                if np.all(np.isnan(data)):
                    checks.append(Check(f"cog_data_{tif.name}", False, f"{tif.name}: all NaN", severity="warning"))
        except Exception as exc:
            errors += 1
            checks.append(Check(f"cog_open_{tif.name}", False, f"{tif.name}: {exc}"))

    if errors == 0:
        checks.append(Check("cog_spot_check", True, f"Spot-checked {min(len(tifs), 10)} COGs — all valid"))
    else:
        checks.append(Check("cog_spot_check", False, f"{errors} COG errors in spot check"))

    return checks


def validate(
    zarr_store: Path | None = None,
    cog_dir: Path | None = None,
    output_subdir: str | None = None,
) -> ValidationReport:
    """Run all validation checks and return a report."""
    out = output_dir(output_subdir)
    store = zarr_store or (out / ZARR_STORE_NAME)
    cogs = cog_dir or (out / "cogs")

    report = ValidationReport(
        zarr_store=str(store),
        cog_dir=str(cogs) if cogs.exists() else None,
        timestamp=datetime.now(timezone.utc).isoformat(),
    )

    # 1. Can we open the store?
    readable = _check_zarr_readable(store)
    report.checks.append(readable)
    if not readable.passed:
        return report

    ds = xr.open_zarr(store)
    try:
        report.checks.extend(_check_expected_variables(ds))
        report.checks.extend(_check_dimensions(ds))
        report.checks.extend(_check_coordinates(ds))
        report.checks.extend(_check_crs(ds))
        report.checks.extend(_check_variable_ranges(ds))
    finally:
        ds.close()

    # 2. COG validation
    report.checks.extend(_check_cogs(cogs))

    return report


def main(argv: list[str] | None = None) -> None:
    """Run validation and print results."""
    parser = argparse.ArgumentParser(description="Validate gridded Zarr store and COG outputs")
    parser.add_argument("--zarr-store", type=Path, default=None, help="Path to Zarr store")
    parser.add_argument("--cog-dir", type=Path, default=None, help="Path to COG directory")
    parser.add_argument("--output-subdir", default=None, help="Processed output subdirectory")
    parser.add_argument("--json", action="store_true", help="Output as JSON")
    parser.add_argument("--save", action="store_true", help="Save report to output dir")
    args = parser.parse_args(argv)

    report = validate(
        zarr_store=args.zarr_store,
        cog_dir=args.cog_dir,
        output_subdir=args.output_subdir,
    )

    if args.json:
        print(json.dumps(report.to_dict(), indent=2))
    else:
        status = "PASSED" if report.passed else "FAILED"
        print(f"\nValidation {status}")
        print(f"  Store: {report.zarr_store}")
        if report.cog_dir:
            print(f"  COGs:  {report.cog_dir}")
        print(f"  Checks: {len(report.checks)} total, {len(report.errors)} errors, {len(report.warnings)} warnings")
        print()
        for check in report.checks:
            icon = "OK" if check.passed else ("WARN" if check.severity == "warning" else "FAIL")
            print(f"  [{icon:4s}] {check.name}: {check.message}")

    if args.save:
        out = output_dir(args.output_subdir)
        report_path = out / "validation_report.json"
        report_path.write_text(json.dumps(report.to_dict(), indent=2))
        print(f"\nReport saved to {report_path}")

    if not report.passed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
