"""Extract GloFAS ensemble discharge forecasts from Copernicus CDS API.

Downloads the latest GloFAS forecast covering GHA, saves as NetCDF.

Usage:
    python -m etl.pipelines.ibf.extract_glofas
    python -m etl.cli extract ibf-glofas-extract

Requires: CDS_API_KEY environment variable.
"""

import time
from datetime import date

from etl.pipelines.ibf.config import (
    CDS_API_KEY,
    CDS_API_URL,
    GLOFAS_AREA,
    GLOFAS_DATASET,
    GLOFAS_LEAD_HOURS,
    GLOFAS_RAW_DIR,
    GLOFAS_SYSTEM_VERSION,
)


def download_forecast(forecast_date: date | None = None) -> str:
    """Download GloFAS forecast for a given date. Returns path to NetCDF."""
    try:
        import cdsapi
    except ImportError:
        raise ImportError("cdsapi not installed. Run: uv pip install cdsapi")

    if not CDS_API_KEY:
        raise ValueError("CDS_API_KEY env var not set. Get one at https://cds.climate.copernicus.eu/")

    if forecast_date is None:
        forecast_date = date.today()

    out_file = GLOFAS_RAW_DIR / f"glofas_forecast_{forecast_date.isoformat()}.nc"

    if out_file.exists() and out_file.stat().st_size > 10000:
        print(f"Cached: {out_file.name} ({out_file.stat().st_size / 1e6:.1f} MB)")
        return str(out_file)

    print(f"Requesting GloFAS forecast for {forecast_date}...")
    client = cdsapi.Client(url=CDS_API_URL, key=CDS_API_KEY)

    request = {
        "system_version": GLOFAS_SYSTEM_VERSION,
        "hydrological_model": "lisflood",
        "product_type": [
            "ensemble_perturbed_forecasts",
            "control_forecast",
        ],
        "variable": "river_discharge_in_the_last_24_hours",
        "year": str(forecast_date.year),
        "month": f"{forecast_date.month:02d}",
        "day": f"{forecast_date.day:02d}",
        "leadtime_hour": GLOFAS_LEAD_HOURS,
        "area": GLOFAS_AREA,
        "data_format": "netcdf",
    }

    client.retrieve(GLOFAS_DATASET, request, str(out_file))
    print(f"Downloaded: {out_file.name} ({out_file.stat().st_size / 1e6:.1f} MB)")
    return str(out_file)


def main():
    t0 = time.time()
    try:
        path = download_forecast()
        print(f"Done in {time.time() - t0:.0f}s — {path}")
    except ImportError as e:
        print(f"SKIP: {e}")
    except ValueError as e:
        print(f"SKIP: {e}")


if __name__ == "__main__":
    main()
