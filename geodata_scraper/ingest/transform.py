"""
Transform layer — in-database SQL transforms after loading.

Runs inside PostGIS: reproject, fix geometries, add spatial indexes,
create materialized views.
"""

import logging

from django.db import connection

from ..settings import scraper_settings

log = logging.getLogger("geodata_scraper.transform")


def transform_layer(schema: str, table: str, geom_col: str = "geom") -> dict:
    """
    Run standard transforms on an ingested PostGIS table:
    1. Fix invalid geometries
    2. Ensure SRID 4326
    3. Add spatial index (if missing)
    4. Update geometry stats
    5. Add area column (for polygons)
    """
    results = {"table": f"{schema}.{table}", "transforms": []}

    with connection.cursor() as cur:
        # 1. Fix invalid geometries
        try:
            cur.execute(f'''
                UPDATE "{schema}"."{table}"
                SET "{geom_col}" = ST_MakeValid("{geom_col}")
                WHERE NOT ST_IsValid("{geom_col}")
            ''')
            fixed = cur.rowcount
            if fixed > 0:
                results["transforms"].append(f"Fixed {fixed} invalid geometries")
                log.info(f"Fixed {fixed} invalid geometries in {schema}.{table}")
        except Exception as e:
            log.warning(f"Geometry fix skipped for {schema}.{table}: {e}")

        # 2. Ensure SRID 4326
        try:
            cur.execute(f'''
                SELECT Find_SRID('{schema}', '{table}', '{geom_col}')
            ''')
            srid = cur.fetchone()[0]
            if srid != scraper_settings.DEFAULT_SRID:
                cur.execute(f'''
                    ALTER TABLE "{schema}"."{table}"
                    ALTER COLUMN "{geom_col}"
                    TYPE geometry USING ST_Transform("{geom_col}", {scraper_settings.DEFAULT_SRID})
                ''')
                cur.execute(f'''
                    SELECT UpdateGeometrySRID('{schema}', '{table}', '{geom_col}', {scraper_settings.DEFAULT_SRID})
                ''')
                results["transforms"].append(f"Reprojected from {srid} to {scraper_settings.DEFAULT_SRID}")
        except Exception as e:
            log.warning(f"SRID check skipped for {schema}.{table}: {e}")

        # 3. Spatial index
        idx_name = f"idx_{table}_{geom_col}"
        try:
            cur.execute("""
                SELECT indexname FROM pg_indexes
                WHERE schemaname = %s AND tablename = %s AND indexdef LIKE '%%gist%%'
            """, [schema, table])
            if not cur.fetchone():
                cur.execute(f'''
                    CREATE INDEX "{idx_name}"
                    ON "{schema}"."{table}" USING GIST ("{geom_col}")
                ''')
                results["transforms"].append("Created spatial index")
        except Exception as e:
            log.warning(f"Spatial index skipped for {schema}.{table}: {e}")

        # 4. Update stats
        try:
            cur.execute(f'ANALYZE "{schema}"."{table}"')
            results["transforms"].append("Updated table statistics")
        except Exception:
            pass

        # 5. Add area_km2 column for polygon layers
        try:
            cur.execute(f'''
                SELECT type FROM geometry_columns
                WHERE f_table_schema = '{schema}'
                AND f_table_name = '{table}'
                AND f_geometry_column = '{geom_col}'
            ''')
            row = cur.fetchone()
            if row and "POLYGON" in (row[0] or "").upper():
                cur.execute(f'''
                    SELECT column_name FROM information_schema.columns
                    WHERE table_schema = %s AND table_name = %s AND column_name = 'area_km2'
                ''', [schema, table])
                if not cur.fetchone():
                    cur.execute(f'''
                        ALTER TABLE "{schema}"."{table}"
                        ADD COLUMN area_km2 double precision
                    ''')
                    cur.execute(f'''
                        UPDATE "{schema}"."{table}"
                        SET area_km2 = ST_Area("{geom_col}"::geography) / 1e6
                    ''')
                    results["transforms"].append("Added area_km2 column")
        except Exception as e:
            log.warning(f"Area calc skipped for {schema}.{table}: {e}")

    log.info(f"TRANSFORM: {schema}.{table} — {len(results['transforms'])} transforms applied")
    return results


def create_unified_view(schema_raw: str, schema_clean: str, iso3_list: list[str]):
    """
    Create a unified materialized view across all countries and admin levels.

    geodata.boundaries_admin0 — all countries admin level 0
    geodata.boundaries_admin1 — all countries admin level 1
    etc.
    """
    from ..regions import get_admin_levels

    max_level = max(get_admin_levels(c) for c in iso3_list)

    with connection.cursor() as cur:
        cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema_clean}"')

        for level in range(max_level + 1):
            tables = []
            for iso3 in iso3_list:
                tbl = f"{iso3.lower()}_admin{level}"
                cur.execute("""
                    SELECT table_name FROM information_schema.tables
                    WHERE table_schema = %s AND table_name = %s
                """, [schema_raw, tbl])
                if cur.fetchone():
                    tables.append(f'SELECT *, \'{iso3.upper()}\' AS country_iso3 FROM "{schema_raw}"."{tbl}"')

            if tables:
                view_name = f"boundaries_admin{level}"
                union_sql = " UNION ALL ".join(tables)
                cur.execute(f'DROP MATERIALIZED VIEW IF EXISTS "{schema_clean}"."{view_name}" CASCADE')
                cur.execute(f'''
                    CREATE MATERIALIZED VIEW "{schema_clean}"."{view_name}" AS
                    {union_sql}
                ''')
                cur.execute(f'''
                    CREATE INDEX ON "{schema_clean}"."{view_name}" USING GIST (geom)
                ''')
                log.info(f"VIEW: {schema_clean}.{view_name} ({len(tables)} countries)")
