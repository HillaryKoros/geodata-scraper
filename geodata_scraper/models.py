import uuid

from django.contrib.gis.db import models
from django.utils import timezone


class DataSource(models.Model):
    """A configured data source to scrape from."""

    PROTOCOL_CHOICES = [
        ("https", "HTTPS"),
        ("api", "REST API"),
        ("ftp", "FTP/SFTP"),
        ("s3", "AWS S3"),
        ("gcs", "Google Cloud Storage"),
        ("file", "Local File"),
    ]
    SOURCE_TYPE_CHOICES = [
        ("gadm", "GADM Admin Boundaries"),
        ("osm", "OpenStreetMap"),
        ("geofabrik", "Geofabrik Extracts"),
        ("generic", "Generic Geodata"),
    ]

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    name = models.CharField(max_length=255, unique=True)
    source_type = models.CharField(max_length=50, choices=SOURCE_TYPE_CHOICES)
    protocol = models.CharField(max_length=20, choices=PROTOCOL_CHOICES, default="https")
    base_url = models.URLField(
        max_length=500,
        blank=True,
        help_text="Base URL pattern. Use {iso3}, {level} as placeholders.",
    )
    auth_config = models.JSONField(
        blank=True, default=dict,
        help_text="Auth credentials: {username, password, token, key}",
    )
    extra_config = models.JSONField(
        blank=True, default=dict,
        help_text="Source-specific config (headers, params, etc.)",
    )
    is_active = models.BooleanField(default=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "Data Source"
        ordering = ["name"]

    def __str__(self):
        return f"{self.name} ({self.get_source_type_display()})"


class ScrapeJob(models.Model):
    """Tracks one run of the ELT pipeline."""

    STATUS_CHOICES = [
        ("pending", "Pending"),
        ("extracting", "Extracting"),
        ("loading", "Loading"),
        ("transforming", "Transforming"),
        ("completed", "Completed"),
        ("failed", "Failed"),
    ]

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    source = models.ForeignKey(DataSource, on_delete=models.CASCADE, related_name="jobs")
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default="pending")
    region = models.CharField(max_length=100, blank=True, help_text="Region or country codes")
    countries = models.JSONField(default=list, help_text="Resolved ISO3 codes")
    admin_levels = models.JSONField(default=list, help_text="Admin levels scraped")

    # Progress
    total_files = models.IntegerField(default=0)
    downloaded_files = models.IntegerField(default=0)
    loaded_tables = models.IntegerField(default=0)
    failed_files = models.IntegerField(default=0)
    bytes_downloaded = models.BigIntegerField(default=0)

    # Timing
    started_at = models.DateTimeField(null=True, blank=True)
    completed_at = models.DateTimeField(null=True, blank=True)

    # Logs
    log = models.TextField(blank=True)
    error = models.TextField(blank=True)

    class Meta:
        verbose_name = "Scrape Job"
        ordering = ["-started_at"]

    def __str__(self):
        return f"Job {self.id.hex[:8]} — {self.source.name} [{self.status}]"

    @property
    def duration(self):
        if self.started_at and self.completed_at:
            return self.completed_at - self.started_at
        return None

    def start(self):
        self.status = "extracting"
        self.started_at = timezone.now()
        self.save(update_fields=["status", "started_at"])

    def complete(self):
        self.status = "completed"
        self.completed_at = timezone.now()
        self.save(update_fields=["status", "completed_at"])

    def fail(self, error_msg: str):
        self.status = "failed"
        self.error = error_msg
        self.completed_at = timezone.now()
        self.save(update_fields=["status", "error", "completed_at"])

    def append_log(self, msg: str):
        self.log += f"[{timezone.now():%H:%M:%S}] {msg}\n"
        self.save(update_fields=["log"])


class IngestedLayer(models.Model):
    """A PostGIS table created by the ELT pipeline."""

    GEOM_TYPE_CHOICES = [
        ("POINT", "Point"),
        ("LINESTRING", "LineString"),
        ("POLYGON", "Polygon"),
        ("MULTIPOINT", "MultiPoint"),
        ("MULTILINESTRING", "MultiLineString"),
        ("MULTIPOLYGON", "MultiPolygon"),
        ("GEOMETRYCOLLECTION", "GeometryCollection"),
    ]

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    job = models.ForeignKey(ScrapeJob, on_delete=models.CASCADE, related_name="layers")
    source = models.ForeignKey(DataSource, on_delete=models.CASCADE, related_name="layers")

    # PostGIS table reference
    db_schema = models.CharField(max_length=100)
    db_table = models.CharField(max_length=255)
    geom_column = models.CharField(max_length=100, default="geom")
    geom_type = models.CharField(max_length=30, choices=GEOM_TYPE_CHOICES, default="MULTIPOLYGON")
    srid = models.IntegerField(default=4326)

    # Metadata
    name = models.CharField(max_length=255, help_text="Human-readable layer name")
    description = models.TextField(blank=True)
    iso3 = models.CharField(max_length=3, blank=True, db_index=True, help_text="Country ISO3 code")
    admin_level = models.IntegerField(null=True, blank=True, db_index=True)
    feature_count = models.IntegerField(default=0)
    properties = models.JSONField(default=list, help_text="Column names in the table")

    # Spatial extent
    bbox = models.PolygonField(srid=4326, null=True, blank=True)

    # Tracking
    source_url = models.URLField(max_length=500, blank=True)
    source_format = models.CharField(max_length=20, blank=True, help_text="gpkg, shp, geojson, etc.")
    file_size = models.BigIntegerField(default=0)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "Ingested Layer"
        unique_together = ["db_schema", "db_table"]
        ordering = ["iso3", "admin_level", "name"]

    def __str__(self):
        level = f" (admin{self.admin_level})" if self.admin_level is not None else ""
        return f"{self.name}{level} — {self.db_schema}.{self.db_table}"

    @property
    def full_table_name(self):
        return f'"{self.db_schema}"."{self.db_table}"'
