"""IBF Trigger Engine — evaluate thresholds and generate alerts.

Reads forecast RP-district mapping + pre-computed impact table.
Applies threshold rules, creates/updates alerts.

Usage:
    python -m etl.pipelines.ibf.trigger
    python -m etl.cli process ibf-trigger
"""

import time
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

from etl.core.config import processed_dir
from etl.pipelines.ibf.config import (
    DEFAULT_THRESHOLDS,
    GLOFAS_PROCESSED_DIR,
    IBF_PROCESSED_DIR,
    closest_rp,
    severity_rank,
)


def evaluate_triggers(
    forecast_date: date | None = None,
    impact_path: Path | None = None,
) -> pd.DataFrame:
    """Evaluate triggers and return alerts DataFrame."""
    if forecast_date is None:
        forecast_date = date.today()

    # Load station forecasts
    station_file = GLOFAS_PROCESSED_DIR / f"station_forecasts_{forecast_date.isoformat()}.csv"
    if not station_file.exists():
        raise FileNotFoundError(f"Station forecasts not found: {station_file}")

    forecasts = pd.read_csv(station_file)
    forecasts["affected_gid2s"] = forecasts["affected_gid2s"].apply(eval)
    print(f"Loaded {len(forecasts)} station forecasts")

    # Load impact table
    if impact_path is None:
        impact_path = IBF_PROCESSED_DIR / "ibf_impact_table.parquet"
        if not impact_path.exists():
            impact_path = processed_dir("") / "ibf_impact_table.parquet"
    if not impact_path.exists():
        impact_path = impact_path.with_suffix(".csv")

    if impact_path.suffix == ".parquet":
        impact = pd.read_parquet(impact_path)
    else:
        impact = pd.read_csv(impact_path)
    print(f"Loaded {len(impact)} impact rows")

    # Build impact lookup: (gid_2, rp) → row
    impact_lookup = {}
    for _, row in impact.iterrows():
        impact_lookup[(row["gid_2"], row["rp"])] = row

    # Evaluate each station forecast against thresholds
    alerts = []
    seen = {}  # gid_2 → best alert so far

    for _, fc in forecasts.iterrows():
        if pd.isna(fc["derived_rp"]) or fc["derived_rp"] is None:
            continue

        rp = int(fc["derived_rp"])
        prob = float(fc.get("probability_above_rp", 0))
        lead = int(fc["lead_time_days"])

        for gid_2 in fc["affected_gid2s"]:
            # Look up pre-computed impact at closest RP
            rp_bucket = closest_rp(rp)
            impact_row = impact_lookup.get((gid_2, rp_bucket))
            if impact_row is None:
                continue

            pop_exposed = int(impact_row["pop_exposed"])

            # Check against each threshold level
            best_match = None
            for threshold in DEFAULT_THRESHOLDS:
                if rp < threshold["min_return_period"]:
                    continue
                if lead < threshold["min_lead_time_days"]:
                    continue
                if prob < threshold["min_probability"]:
                    continue
                if pop_exposed < threshold["min_pop_exposed"]:
                    continue

                if best_match is None or severity_rank(threshold["severity"]) > severity_rank(best_match["severity"]):
                    best_match = threshold

            if best_match is None:
                continue

            # Keep only the highest severity alert per district
            existing = seen.get(gid_2)
            if existing and severity_rank(existing["severity"]) >= severity_rank(best_match["severity"]):
                continue

            alert = {
                "gid_2": gid_2,
                "country": impact_row["country"],
                "admin1": impact_row["admin1"],
                "admin2": impact_row["admin2"],
                "severity": best_match["severity"],
                "return_period": rp,
                "lead_time_days": lead,
                "discharge_m3s": float(fc["discharge_m3s"]),
                "probability": prob,
                "station_id": fc["station_id"],
                "station_name": fc["station_name"],
                "pop_exposed": pop_exposed,
                "pop_exposed_pct": float(impact_row["pop_exposed_pct"]),
                "flood_area_km2": float(impact_row["flood_area_km2"]),
                "cropland_flooded_km2": float(impact_row.get("cropland_flooded_km2", 0)),
                "mean_depth_m": float(impact_row["mean_depth_m"]),
                "forecast_date": forecast_date.isoformat(),
                "expires_at": (forecast_date + timedelta(days=lead + 2)).isoformat(),
            }
            seen[gid_2] = alert

    alerts = list(seen.values())
    df = pd.DataFrame(alerts)

    if len(df) > 0:
        out_file = IBF_PROCESSED_DIR / f"alerts_{forecast_date.isoformat()}.csv"
        df.to_csv(out_file, index=False)
        print(f"\n{len(df)} alerts generated → {out_file.name}")
        print(f"  Alert: {len(df[df['severity'] == 'alert'])}")
        print(f"  Warning: {len(df[df['severity'] == 'warning'])}")
        print(f"  Watch: {len(df[df['severity'] == 'watch'])}")
    else:
        print("\nNo alerts triggered")

    return df


def main():
    t0 = time.time()
    try:
        evaluate_triggers()
        print(f"Done in {time.time() - t0:.0f}s")
    except FileNotFoundError as e:
        print(f"SKIP: {e}")


if __name__ == "__main__":
    main()
