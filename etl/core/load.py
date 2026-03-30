"""Load layer — PostGIS primary, DuckDB backup.

Shared functions used by all extractors to load data into databases.
"""

import duckdb
import geopandas as gpd
from pathlib import Path
from tqdm import tqdm
from etl.core.config import ROOT, PG_URL

DB_PATH = ROOT / "spatial.duckdb"


# --- DuckDB ---

def get_db():
    """Get a DuckDB connection with spatial extension."""
    con = duckdb.connect(str(DB_PATH))
    con.install_extension("spatial")
    con.load_extension("spatial")
    return con


def duckdb_load_parquet(table_name: str, parquet_path: Path):
    """Load a GeoParquet into DuckDB with proper geometry conversion."""
    con = get_db()
    con.execute(f"DROP TABLE IF EXISTS {table_name}")
    gdf = gpd.read_parquet(parquet_path)
    df = gdf.copy()
    df["geometry"] = df["geometry"].apply(lambda g: g.wkb)
    con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM df")
    con.execute(
        f"ALTER TABLE {table_name} ALTER geometry TYPE GEOMETRY "
        f"USING ST_GeomFromWKB(geometry)"
    )
    count = con.execute(f"SELECT count(*) FROM {table_name}").fetchone()[0]
    print(f"duckdb: {table_name} ({count} rows)")
    con.close()


def duckdb_query(sql: str):
    """Run a SQL query on local DuckDB."""
    con = get_db()
    result = con.execute(sql).fetchdf()
    con.close()
    return result


def duckdb_tables():
    """List tables in local DuckDB."""
    con = get_db()
    tables = con.execute("SHOW TABLES").fetchdf()
    con.close()
    return tables


# --- PostGIS ---

def postgis_load_parquet(table_name: str, parquet_path: Path):
    """Load a GeoParquet into PostGIS."""
    from sqlalchemy import create_engine

    gdf = gpd.read_parquet(parquet_path)
    engine = create_engine(PG_URL)
    gdf.to_postgis(table_name, engine, if_exists="replace", index=False)
    print(f"postgis: {table_name} ({len(gdf)} rows)")
    engine.dispose()


def postgis_query(sql: str):
    """Run a SQL query on PostGIS."""
    from sqlalchemy import create_engine

    engine = create_engine(PG_URL)
    result = gpd.read_postgis(sql, engine, geom_col="geometry")
    engine.dispose()
    return result


# --- Batch load ---

def load_parquets(parquet_files: list[Path], postgis: bool = True, duckdb_backup: bool = True):
    """Load multiple parquet files to PostGIS and/or DuckDB."""
    if postgis:
        print("Loading to PostGIS...")
        for f in tqdm(parquet_files, desc="PostGIS", unit="table"):
            try:
                postgis_load_parquet(f.stem, f)
            except Exception as e:
                print(f"  PostGIS failed for {f.stem}: {e}")

    if duckdb_backup:
        print("Loading to DuckDB...")
        for f in tqdm(parquet_files, desc="DuckDB", unit="table"):
            try:
                duckdb_load_parquet(f.stem, f)
            except Exception as e:
                print(f"  DuckDB failed for {f.stem}: {e}")
