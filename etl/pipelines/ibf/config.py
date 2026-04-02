"""IBF-specific configuration — GloFAS, thresholds, CDS API."""

import os

from etl.core.config import AOI_BBOX, processed_dir, raw_dir

# ---------------------------------------------------------------------------
# Copernicus Climate Data Store (CDS) API
# ---------------------------------------------------------------------------
CDS_API_URL = os.getenv("CDS_API_URL", "https://cds.climate.copernicus.eu/api")
CDS_API_KEY = os.getenv("CDS_API_KEY", "")

# GloFAS forecast parameters
GLOFAS_SYSTEM_VERSION = os.getenv("GLOFAS_SYSTEM_VERSION", "operational")
GLOFAS_DATASET = "cems-glofas-forecast"
GLOFAS_HISTORICAL_DATASET = "cems-glofas-historical"
GLOFAS_LEAD_HOURS = [str(h) for h in range(24, 361, 24)]  # 1-15 days
GLOFAS_AREA = [AOI_BBOX[3], AOI_BBOX[0], AOI_BBOX[1], AOI_BBOX[2]]  # N, W, S, E

# How many days ahead to consider for triggers
IBF_TRIGGER_LEAD_DAYS = int(os.getenv("IBF_TRIGGER_LEAD_DAYS", "7"))

# ---------------------------------------------------------------------------
# Return periods used in pre-computed impact tables
# ---------------------------------------------------------------------------
RETURN_PERIODS = [10, 20, 50, 75, 100]

# ---------------------------------------------------------------------------
# Default trigger thresholds (severity → conditions)
# These seed the TriggerThreshold model on first run.
# ---------------------------------------------------------------------------
DEFAULT_THRESHOLDS = [
    {
        "severity": "watch",
        "min_return_period": 10,
        "min_pop_exposed": 1000,
        "min_probability": 0.3,
        "min_lead_time_days": 1,
    },
    {
        "severity": "warning",
        "min_return_period": 20,
        "min_pop_exposed": 5000,
        "min_probability": 0.5,
        "min_lead_time_days": 1,
    },
    {
        "severity": "alert",
        "min_return_period": 50,
        "min_pop_exposed": 10000,
        "min_probability": 0.5,
        "min_lead_time_days": 1,
    },
]

# ---------------------------------------------------------------------------
# GloFAS reporting stations for GHA region
# station_id → {name, river, lat, lon, rp_thresholds, affected_gid2s}
# This is the initial seed — full list maintained in DB after first load.
# ---------------------------------------------------------------------------
GLOFAS_STATIONS = {
    "G0001": {
        "name": "Khartoum",
        "river": "Blue Nile",
        "lat": 15.60,
        "lon": 32.55,
        "rp_thresholds": {2: 4500, 5: 6000, 10: 7200, 20: 8500, 50: 10500, 100: 12000},
        "affected_gid2s": ["SDN.7.1_1", "SDN.7.2_1", "SDN.7.3_1"],
    },
    "G0002": {
        "name": "Juba",
        "river": "White Nile",
        "lat": 4.85,
        "lon": 31.60,
        "rp_thresholds": {2: 600, 5: 900, 10: 1100, 20: 1400, 50: 1800, 100: 2100},
        "affected_gid2s": ["SSD.3.1_1", "SSD.3.2_1"],
    },
    "G0003": {
        "name": "Garissa",
        "river": "Tana",
        "lat": -0.45,
        "lon": 39.64,
        "rp_thresholds": {2: 200, 5: 400, 10: 600, 20: 850, 50: 1200, 100: 1500},
        "affected_gid2s": ["KEN.7.1_1", "KEN.7.2_1"],
    },
    "G0004": {
        "name": "Belet Weyne",
        "river": "Shabelle",
        "lat": 4.74,
        "lon": 45.20,
        "rp_thresholds": {2: 150, 5: 300, 10: 450, 20: 650, 50: 900, 100: 1100},
        "affected_gid2s": ["SOM.5.1_1", "SOM.5.2_1"],
    },
    "G0005": {
        "name": "Gambela",
        "river": "Baro",
        "lat": 8.25,
        "lon": 34.58,
        "rp_thresholds": {2: 300, 5: 500, 10: 700, 20: 950, 50: 1300, 100: 1600},
        "affected_gid2s": ["ETH.4.1_1"],
    },
}

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
GLOFAS_RAW_DIR = raw_dir("glofas")
GLOFAS_PROCESSED_DIR = processed_dir("glofas")
IBF_PROCESSED_DIR = processed_dir("ibf")


def closest_rp(derived_rp: float) -> int:
    """Map a continuous RP value to the nearest pre-computed RP bucket."""
    return min(RETURN_PERIODS, key=lambda x: abs(x - derived_rp))


def severity_rank(severity: str) -> int:
    """Numeric rank for severity comparison."""
    return {"watch": 1, "warning": 2, "alert": 3}.get(severity, 0)
