"""Export IBF alerts as CSV, GeoJSON.

Usage:
    python -m etl.pipelines.ibf.export
    python -m etl.cli process ibf-export
"""

import time
from datetime import date

import geopandas as gpd
import pandas as pd

from etl.core.config import raw_dir
from etl.pipelines.ibf.config import IBF_PROCESSED_DIR


def export_alerts(forecast_date: date | None = None):
    """Export latest alerts as CSV and GeoJSON."""
    if forecast_date is None:
        forecast_date = date.today()

    alert_file = IBF_PROCESSED_DIR / f"alerts_{forecast_date.isoformat()}.csv"
    if not alert_file.exists():
        # Find most recent alerts file
        alert_files = sorted(IBF_PROCESSED_DIR.glob("alerts_*.csv"), reverse=True)
        if not alert_files:
            print("No alert files found")
            return
        alert_file = alert_files[0]
        print(f"Using most recent: {alert_file.name}")

    alerts = pd.read_csv(alert_file)
    print(f"Loaded {len(alerts)} alerts from {alert_file.name}")

    if len(alerts) == 0:
        print("No alerts to export")
        return

    # Load admin2 geometries for GeoJSON
    adm2_path = raw_dir("gadm") / "igad_adm2.parquet"
    if adm2_path.exists():
        adm2 = gpd.read_parquet(adm2_path)
        # Simplify for export
        adm2["geometry"] = adm2.geometry.simplify(0.005)

        merged = adm2.merge(alerts, left_on="GID_2", right_on="gid_2", how="inner")
        geo = gpd.GeoDataFrame(merged, geometry="geometry", crs="EPSG:4326")

        geojson_path = IBF_PROCESSED_DIR / f"alerts_{forecast_date.isoformat()}.geojson"
        geo.to_file(geojson_path, driver="GeoJSON")
        print(f"GeoJSON: {geojson_path.name} ({geojson_path.stat().st_size / 1e3:.0f} KB)")

    print("Export complete")


def main():
    t0 = time.time()
    export_alerts()
    print(f"Done in {time.time() - t0:.0f}s")


if __name__ == "__main__":
    main()
