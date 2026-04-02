"""Process GloFAS forecast: extract station discharge, derive return periods.

Reads the downloaded NetCDF, extracts discharge at each GloFAS reporting
station, compares to RP thresholds, and maps to downstream admin2 districts.

Usage:
    python -m etl.pipelines.ibf.process_glofas
    python -m etl.cli process ibf-glofas-process
"""

import time
from datetime import date

import numpy as np
import pandas as pd

from etl.pipelines.ibf.config import (
    GLOFAS_PROCESSED_DIR,
    GLOFAS_RAW_DIR,
    GLOFAS_STATIONS,
    IBF_TRIGGER_LEAD_DAYS,
)


def derive_rp_from_thresholds(discharge: float, thresholds: dict) -> int | None:
    """Find the highest RP that the discharge exceeds."""
    exceeded = None
    for rp_str, threshold_val in sorted(thresholds.items(), key=lambda x: int(x[0])):
        rp = int(rp_str)
        if discharge >= threshold_val:
            exceeded = rp
    return exceeded


def process_forecast(forecast_date: date | None = None) -> pd.DataFrame:
    """Process GloFAS forecast and return station-level RP mapping."""
    try:
        import xarray as xr
    except ImportError:
        raise ImportError("xarray not installed. Run: uv pip install xarray netcdf4")

    if forecast_date is None:
        forecast_date = date.today()

    nc_file = GLOFAS_RAW_DIR / f"glofas_forecast_{forecast_date.isoformat()}.nc"
    if not nc_file.exists():
        raise FileNotFoundError(f"Forecast file not found: {nc_file}")

    print(f"Processing {nc_file.name}...")
    ds = xr.open_dataset(nc_file)

    results = []
    for station_id, info in GLOFAS_STATIONS.items():
        lat, lon = info["lat"], info["lon"]
        thresholds = info["rp_thresholds"]

        # Extract nearest grid cell
        try:
            point = ds.sel(latitude=lat, longitude=lon, method="nearest")
        except Exception:
            print(f"  {station_id}: could not extract — skipping")
            continue

        # Get discharge variable (name varies by dataset version)
        dis_var = None
        for var_name in ["dis24", "dis", "river_discharge_in_the_last_24_hours"]:
            if var_name in point.data_vars:
                dis_var = var_name
                break
        if dis_var is None:
            print(f"  {station_id}: no discharge variable found")
            continue

        dis = point[dis_var]

        # Process each lead time
        for step_idx, step in enumerate(dis.coords.get("step", dis.coords.get("time", []))):
            lead_days = step_idx + 1
            if lead_days > IBF_TRIGGER_LEAD_DAYS:
                break

            # Get ensemble statistics
            if "number" in dis.dims:
                values = dis.sel(step=step).values
                ensemble_mean = float(np.nanmean(values))
                ensemble_max = float(np.nanmax(values))
                ensemble_min = float(np.nanmin(values))
                # Count ensemble members exceeding RP thresholds
                n_members = len(values)
            else:
                val = float(dis.sel(step=step).values) if "step" in dis.dims else float(dis.values)
                ensemble_mean = val
                ensemble_max = val
                ensemble_min = val
                n_members = 1

            derived_rp = derive_rp_from_thresholds(ensemble_mean, thresholds)

            # Probability = fraction of ensemble above derived RP threshold
            prob = 0.0
            if derived_rp and n_members > 1:
                rp_threshold = thresholds.get(str(derived_rp), thresholds.get(derived_rp, 0))
                prob = float(np.sum(values >= rp_threshold)) / n_members

            results.append({
                "station_id": station_id,
                "station_name": info["name"],
                "river": info["river"],
                "lat": lat,
                "lon": lon,
                "lead_time_days": lead_days,
                "discharge_m3s": round(ensemble_mean, 1),
                "discharge_max_m3s": round(ensemble_max, 1),
                "discharge_min_m3s": round(ensemble_min, 1),
                "derived_rp": derived_rp,
                "probability_above_rp": round(prob, 2),
                "affected_gid2s": info["affected_gid2s"],
            })

    ds.close()

    df = pd.DataFrame(results)
    out_file = GLOFAS_PROCESSED_DIR / f"station_forecasts_{forecast_date.isoformat()}.csv"
    df.to_csv(out_file, index=False)
    print(f"Processed {len(df)} station-lead combinations → {out_file.name}")

    return df


def main():
    t0 = time.time()
    try:
        df = process_forecast()
        rp_hits = df[df["derived_rp"].notna()]
        print(f"\n{len(rp_hits)} exceedances detected across {rp_hits['station_id'].nunique()} stations")
        print(f"Done in {time.time() - t0:.0f}s")
    except (ImportError, FileNotFoundError) as e:
        print(f"SKIP: {e}")


if __name__ == "__main__":
    main()
