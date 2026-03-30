"""
geodata — GIS ELT pipeline for PostGIS

Extract geodata from any source (HTTPS, API, FTP),
Load raw into PostGIS via ogr2ogr,
Transform in-database with SQL.
"""

__version__ = "0.1.0"

default_app_config = "geodata.apps.GeodataScraperConfig"
