"""
Load layer — ogr2ogr pipeline to ingest geodata into PostGIS.

Handles: GeoPackage, Shapefile, GeoJSON → PostGIS tables.
GeoPackage files with multiple layers (GADM) are split into
separate tables per admin level.
"""

import logging
import subprocess
from pathlib import Path

from django.db import connection

from ..settings import scraper_settings

log = logging.getLogger("geodata.ingest")


def _get_pg_connstring() -> str:
    """Build OGR PostgreSQL connection string from Django DB settings."""
    db = connection.settings_dict
    parts = [f"dbname='{db['NAME']}'"]
    if db.get("HOST"):
        parts.append(f"host='{db['HOST']}'")
    if db.get("PORT"):
        parts.append(f"port='{db['PORT']}'")
    if db.get("USER"):
        parts.append(f"user='{db['USER']}'")
    if db.get("PASSWORD"):
        parts.append(f"password='{db['PASSWORD']}'")
    return f"PG:{' '.join(parts)}"


def _run_ogr2ogr(args: list[str]) -> subprocess.CompletedProcess:
    """Run ogr2ogr with given arguments."""
    cmd = [scraper_settings.OGR2OGR_BIN] + args
    log.debug(f"Running: {' '.join(cmd)}")

    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        timeout=600,
    )

    if result.returncode != 0:
        log.error(f"ogr2ogr failed: {result.stderr}")
        raise RuntimeError(f"ogr2ogr error: {result.stderr}")

    return result


def _list_gpkg_layers(gpkg_path: Path) -> list[str]:
    """List layer names inside a GeoPackage."""
    result = subprocess.run(
        ["ogrinfo", "-so", "-q", str(gpkg_path)],
        capture_output=True,
        text=True,
    )
    layers = []
    for line in result.stdout.strip().splitlines():
        # Format: "1: layer_name (Multi Polygon)"
        parts = line.strip().split(":")
        if len(parts) >= 2:
            name = parts[1].strip().split("(")[0].strip()
            if name:
                layers.append(name)
    return layers


def _get_feature_count(schema: str, table: str) -> int:
    """Get row count for a PostGIS table."""
    try:
        with connection.cursor() as cur:
            cur.execute(f'SELECT COUNT(*) FROM "{schema}"."{table}"')
            return cur.fetchone()[0]
    except Exception:
        return 0


def _get_table_columns(schema: str, table: str) -> list[str]:
    """Get column names for a PostGIS table."""
    try:
        with connection.cursor() as cur:
            cur.execute(
                """
                SELECT column_name FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                ORDER BY ordinal_position
            """,
                [schema, table],
            )
            return [row[0] for row in cur.fetchall()]
    except Exception:
        return []


def _get_table_extent(schema: str, table: str, geom_col: str = "geom") -> str | None:
    """Get WKT extent polygon for a PostGIS table."""
    try:
        with connection.cursor() as cur:
            cur.execute(f'''
                SELECT ST_AsText(ST_Extent("{geom_col}"))
                FROM "{schema}"."{table}"
            ''')
            row = cur.fetchone()
            return row[0] if row else None
    except Exception:
        return None


def load_to_postgis(
    file_path: Path,
    schema: str,
    table_name: str,
    srid: int = 4326,
    overwrite: bool = True,
    source_layer: str | None = None,
) -> dict:
    """
    Load a single geodata file into PostGIS via ogr2ogr.

    Returns metadata dict: {schema, table, feature_count, columns, geom_type}
    """
    pg_conn = _get_pg_connstring()

    # Ensure schema exists
    with connection.cursor() as cur:
        cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')

    args = [
        "-f",
        "PostgreSQL",
        pg_conn,
        str(file_path),
        "-nln",
        f"{schema}.{table_name}",
        "-t_srs",
        f"EPSG:{srid}",
        "-lco",
        "GEOMETRY_NAME=geom",
        "-lco",
        f"SCHEMA={schema}",
        "-lco",
        "FID=ogc_fid",
        "-lco",
        "SPATIAL_INDEX=YES",
        "--config",
        "OGR_TRUNCATE",
        "NO",
    ]

    if overwrite:
        args.append("-overwrite")

    if source_layer:
        args.extend(["-sql", f'SELECT * FROM "{source_layer}"'])

    _run_ogr2ogr(args)

    feature_count = _get_feature_count(schema, table_name)
    columns = _get_table_columns(schema, table_name)

    log.info(f"LOADED: {schema}.{table_name} ({feature_count} features)")

    return {
        "schema": schema,
        "table": table_name,
        "feature_count": feature_count,
        "columns": columns,
        "srid": srid,
    }


def load_gadm_gpkg(
    gpkg_path: Path,
    iso3: str,
    schema: str | None = None,
) -> list[dict]:
    """
    Load a GADM GeoPackage (all admin levels) into PostGIS.

    Creates one table per admin level:
        schema.{iso3_lower}_admin0, schema.{iso3_lower}_admin1, ...

    Returns list of metadata dicts.
    """
    schema = schema or scraper_settings.DB_SCHEMA_RAW
    iso3_lower = iso3.lower()
    layers = _list_gpkg_layers(gpkg_path)
    results = []

    for layer_name in layers:
        # GADM layer names: "ADM_ADM_0", "ADM_ADM_1", etc.
        # Detect admin level from layer name
        admin_level = _parse_admin_level(layer_name)
        table_name = f"{iso3_lower}_admin{admin_level}"

        try:
            meta = load_to_postgis(
                file_path=gpkg_path,
                schema=schema,
                table_name=table_name,
                source_layer=layer_name,
            )
            meta["iso3"] = iso3.upper()
            meta["admin_level"] = admin_level
            meta["source_layer"] = layer_name
            results.append(meta)

        except Exception as e:
            log.error(f"Failed to load {layer_name} → {schema}.{table_name}: {e}")
            results.append(
                {
                    "schema": schema,
                    "table": table_name,
                    "iso3": iso3.upper(),
                    "admin_level": admin_level,
                    "error": str(e),
                }
            )

    return results


def load_geojson(
    geojson_path: Path,
    iso3: str,
    admin_level: int,
    schema: str | None = None,
) -> dict:
    """Load a single GeoJSON file into PostGIS."""
    schema = schema or scraper_settings.DB_SCHEMA_RAW
    table_name = f"{iso3.lower()}_admin{admin_level}"

    return load_to_postgis(
        file_path=geojson_path,
        schema=schema,
        table_name=table_name,
    )


def _parse_admin_level(layer_name: str) -> int:
    """Extract admin level number from GADM layer name."""
    # Patterns: "ADM_ADM_0", "gadm41_KEN_0", "admin_0", etc.
    import re

    match = re.search(r"(\d+)$", layer_name)
    if match:
        return int(match.group(1))
    return 0
