"""CLI entrypoint — run individual extract/process/query steps.

Usage:
    uv run python run.py extract gadm
    uv run python run.py extract all
    uv run python run.py process jrc-flood
    uv run python run.py query "SELECT count(*) FROM igad_adm0"
    uv run python run.py tables
"""

import click
import importlib

EXTRACTORS = {
    "gadm":        "etl.pipelines.gadm.extract",
    "gha":         "etl.pipelines.gadm.gha_boundary",
    "jrc-flood":   "etl.pipelines.jrc.extract_flood",
    "jrc-water":   "etl.pipelines.jrc.extract_water",
    "hydrobasins": "etl.pipelines.hydrosheds.extract_basins",
    "hydrorivers": "etl.pipelines.hydrosheds.extract_rivers",
    "osm-water":   "etl.pipelines.osm.extract_water",
    "osm-roads":   "etl.pipelines.osm.extract_roads",
    "dem":         "etl.pipelines.dem.extract",
    "buildings":   "etl.pipelines.buildings.extract",
    "population":  "etl.pipelines.population.extract",
    "gridded-manifest": "etl.pipelines.gridded.extract_manifest",
    "gridded-qa": "etl.pipelines.gridded.validate_inputs",
}

PROCESSORS = {
    "jrc-flood": "etl.pipelines.jrc.process_flood",
    "gridded-zarr": "etl.pipelines.gridded.process_zarr",
    "gridded-cogs": "etl.pipelines.gridded.process_cogs",
    "gridded-validate": "etl.pipelines.gridded.validate_output",
}


@click.group()
def cli():
    """spatial-db: local-first spatial data pipelines for GHA."""


@cli.command()
@click.argument("source", type=click.Choice(list(EXTRACTORS.keys()) + ["all"]))
def extract(source):
    """Run an extractor."""
    if source == "all":
        for name, mod_name in EXTRACTORS.items():
            print(f"\n{'=' * 40}\n{name}\n{'=' * 40}")
            importlib.import_module(mod_name).main()
    else:
        importlib.import_module(EXTRACTORS[source]).main()


@cli.command()
@click.argument("source", type=click.Choice(list(PROCESSORS.keys()) + ["all"]))
def process(source):
    """Run a processor."""
    if source == "all":
        for name, mod_name in PROCESSORS.items():
            print(f"\n{'=' * 40}\n{name}\n{'=' * 40}")
            importlib.import_module(mod_name).main()
    else:
        importlib.import_module(PROCESSORS[source]).main()


@cli.command()
@click.argument("sql")
def query(sql):
    """Run SQL against local DuckDB."""
    from etl.core.load import duckdb_query
    print(duckdb_query(sql))


@cli.command()
def tables():
    """List DuckDB tables."""
    from etl.core.load import duckdb_tables
    print(duckdb_tables())


if __name__ == "__main__":
    cli()
