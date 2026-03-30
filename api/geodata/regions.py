"""
Region presets — named groups of ISO3 country codes.

Usage:
    from geodata.regions import get_countries
    get_countries("igad_plus")  # → ["DJI", "ERI", "ETH", ...]
    get_countries("KEN,TZA")    # → ["KEN", "TZA"]
"""

REGIONS = {
    # IGAD member states
    "igad": ["DJI", "ERI", "ETH", "KEN", "SOM", "SSD", "SDN", "UGA"],
    # IGAD + Great Lakes
    "igad_plus": [
        "DJI",
        "ERI",
        "ETH",
        "KEN",
        "SOM",
        "SSD",
        "SDN",
        "UGA",
        "BDI",
        "RWA",
        "TZA",
    ],
    # East African Community
    "eac": ["BDI", "COD", "KEN", "RWA", "SSD", "TZA", "UGA"],
    # Horn of Africa
    "horn": ["DJI", "ERI", "ETH", "SOM"],
    # SADC
    "sadc": [
        "AGO",
        "BWA",
        "COM",
        "COD",
        "SWZ",
        "LSO",
        "MDG",
        "MWI",
        "MUS",
        "MOZ",
        "NAM",
        "SYC",
        "ZAF",
        "TZA",
        "ZMB",
        "ZWE",
    ],
    # ECOWAS
    "ecowas": [
        "BEN",
        "BFA",
        "CPV",
        "CIV",
        "GMB",
        "GHA",
        "GIN",
        "GNB",
        "LBR",
        "MLI",
        "NER",
        "NGA",
        "SEN",
        "SLE",
        "TGO",
    ],
    # All Africa
    "africa": [
        "DZA",
        "AGO",
        "BEN",
        "BWA",
        "BFA",
        "BDI",
        "CMR",
        "CPV",
        "CAF",
        "TCD",
        "COM",
        "COG",
        "COD",
        "CIV",
        "DJI",
        "EGY",
        "GNQ",
        "ERI",
        "SWZ",
        "ETH",
        "GAB",
        "GMB",
        "GHA",
        "GIN",
        "GNB",
        "KEN",
        "LSO",
        "LBR",
        "LBY",
        "MDG",
        "MWI",
        "MLI",
        "MRT",
        "MUS",
        "MAR",
        "MOZ",
        "NAM",
        "NER",
        "NGA",
        "RWA",
        "STP",
        "SEN",
        "SYC",
        "SLE",
        "SOM",
        "ZAF",
        "SSD",
        "SDN",
        "TZA",
        "TGO",
        "TUN",
        "UGA",
        "ZMB",
        "ZWE",
    ],
}

# GADM admin level counts per country (known maximums)
GADM_ADMIN_LEVELS = {
    "DJI": 3,
    "ERI": 3,
    "ETH": 4,
    "KEN": 4,
    "SOM": 3,
    "SSD": 3,
    "SDN": 3,
    "UGA": 4,
    "BDI": 4,
    "RWA": 4,
    "TZA": 4,
    "COD": 3,
    "NGA": 3,
    "ZAF": 3,
    "EGY": 3,
    "GHA": 3,
    "MOZ": 3,
    "MDG": 4,
    "CMR": 3,
    "AGO": 3,
}
# Default if country not in the map above
GADM_ADMIN_LEVELS_DEFAULT = 3


def get_countries(region_or_codes: str) -> list[str]:
    """Resolve a region name or comma-separated ISO3 codes to a list."""
    key = region_or_codes.strip().lower()
    if key in REGIONS:
        return REGIONS[key]
    # Treat as comma-separated ISO3 codes
    codes = [c.strip().upper() for c in region_or_codes.split(",") if c.strip()]
    if not codes:
        raise ValueError(f"Unknown region or empty country list: {region_or_codes}")
    return codes


def get_admin_levels(iso3: str) -> int:
    """Return max admin level for a country."""
    return GADM_ADMIN_LEVELS.get(iso3.upper(), GADM_ADMIN_LEVELS_DEFAULT)
