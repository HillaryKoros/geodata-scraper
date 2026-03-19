import uuid
import django.contrib.gis.db.models.fields
from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    initial = True
    dependencies = []

    operations = [
        migrations.CreateModel(
            name="DataSource",
            fields=[
                ("id", models.UUIDField(default=uuid.uuid4, editable=False, primary_key=True, serialize=False)),
                ("name", models.CharField(max_length=255, unique=True)),
                ("source_type", models.CharField(
                    choices=[("gadm", "GADM Admin Boundaries"), ("osm", "OpenStreetMap"), ("geofabrik", "Geofabrik Extracts"), ("generic", "Generic Geodata")],
                    max_length=50,
                )),
                ("protocol", models.CharField(
                    choices=[("https", "HTTPS"), ("api", "REST API"), ("ftp", "FTP/SFTP"), ("s3", "AWS S3"), ("gcs", "Google Cloud Storage"), ("file", "Local File")],
                    default="https", max_length=20,
                )),
                ("base_url", models.URLField(blank=True, max_length=500)),
                ("auth_config", models.JSONField(blank=True, default=dict)),
                ("extra_config", models.JSONField(blank=True, default=dict)),
                ("is_active", models.BooleanField(default=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
            ],
            options={"verbose_name": "Data Source", "ordering": ["name"]},
        ),
        migrations.CreateModel(
            name="ScrapeJob",
            fields=[
                ("id", models.UUIDField(default=uuid.uuid4, editable=False, primary_key=True, serialize=False)),
                ("status", models.CharField(
                    choices=[("pending", "Pending"), ("extracting", "Extracting"), ("loading", "Loading"), ("transforming", "Transforming"), ("completed", "Completed"), ("failed", "Failed")],
                    default="pending", max_length=20,
                )),
                ("region", models.CharField(blank=True, max_length=100)),
                ("countries", models.JSONField(default=list)),
                ("admin_levels", models.JSONField(default=list)),
                ("total_files", models.IntegerField(default=0)),
                ("downloaded_files", models.IntegerField(default=0)),
                ("loaded_tables", models.IntegerField(default=0)),
                ("failed_files", models.IntegerField(default=0)),
                ("bytes_downloaded", models.BigIntegerField(default=0)),
                ("started_at", models.DateTimeField(blank=True, null=True)),
                ("completed_at", models.DateTimeField(blank=True, null=True)),
                ("log", models.TextField(blank=True)),
                ("error", models.TextField(blank=True)),
                ("source", models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name="jobs", to="geodata_scraper.datasource")),
            ],
            options={"verbose_name": "Scrape Job", "ordering": ["-started_at"]},
        ),
        migrations.CreateModel(
            name="IngestedLayer",
            fields=[
                ("id", models.UUIDField(default=uuid.uuid4, editable=False, primary_key=True, serialize=False)),
                ("db_schema", models.CharField(max_length=100)),
                ("db_table", models.CharField(max_length=255)),
                ("geom_column", models.CharField(default="geom", max_length=100)),
                ("geom_type", models.CharField(
                    choices=[("POINT", "Point"), ("LINESTRING", "LineString"), ("POLYGON", "Polygon"), ("MULTIPOINT", "MultiPoint"), ("MULTILINESTRING", "MultiLineString"), ("MULTIPOLYGON", "MultiPolygon"), ("GEOMETRYCOLLECTION", "GeometryCollection")],
                    default="MULTIPOLYGON", max_length=30,
                )),
                ("srid", models.IntegerField(default=4326)),
                ("name", models.CharField(max_length=255)),
                ("description", models.TextField(blank=True)),
                ("iso3", models.CharField(blank=True, db_index=True, max_length=3)),
                ("admin_level", models.IntegerField(blank=True, db_index=True, null=True)),
                ("feature_count", models.IntegerField(default=0)),
                ("properties", models.JSONField(default=list)),
                ("bbox", django.contrib.gis.db.models.fields.PolygonField(blank=True, null=True, srid=4326)),
                ("source_url", models.URLField(blank=True, max_length=500)),
                ("source_format", models.CharField(blank=True, max_length=20)),
                ("file_size", models.BigIntegerField(default=0)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                ("job", models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name="layers", to="geodata_scraper.scrapejob")),
                ("source", models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name="layers", to="geodata_scraper.datasource")),
            ],
            options={
                "verbose_name": "Ingested Layer",
                "ordering": ["iso3", "admin_level", "name"],
                "unique_together": {("db_schema", "db_table")},
            },
        ),
    ]
