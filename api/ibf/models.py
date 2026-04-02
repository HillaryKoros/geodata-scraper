"""IBF Decision Support System models.

8 models covering: hazard types, admin units, pre-computed impacts,
GloFAS forecast stations, forecast runs, station forecasts,
trigger thresholds, and alerts.
"""

import uuid

from django.contrib.gis.db import models
from django.utils import timezone


class HazardType(models.Model):
    """Extensible hazard registry — flood now, drought/cyclone later."""

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    code = models.CharField(max_length=30, unique=True)
    name = models.CharField(max_length=100)
    description = models.TextField(blank=True)
    return_periods = models.JSONField(
        default=list, help_text="e.g. [10, 20, 50, 75, 100]"
    )
    is_active = models.BooleanField(default=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["code"]

    def __str__(self):
        return self.name


class AdminUnit(models.Model):
    """Admin2 district — the unit of impact assessment."""

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    gid_2 = models.CharField(max_length=30, unique=True, db_index=True)
    country = models.CharField(max_length=100)
    iso3 = models.CharField(max_length=3, db_index=True)
    admin1_name = models.CharField(max_length=200)
    admin2_name = models.CharField(max_length=200)
    geom = models.MultiPolygonField(srid=4326, null=True, blank=True)
    pop_total = models.IntegerField(default=0)
    area_km2 = models.FloatField(default=0)
    extra = models.JSONField(default=dict, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["iso3", "admin1_name", "admin2_name"]
        verbose_name = "Admin Unit"

    def __str__(self):
        return f"{self.admin2_name}, {self.admin1_name} ({self.iso3})"


class ImpactEstimate(models.Model):
    """Pre-computed impact for one district at one return period."""

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    admin_unit = models.ForeignKey(
        AdminUnit, on_delete=models.CASCADE, related_name="impacts"
    )
    hazard_type = models.ForeignKey(
        HazardType, on_delete=models.CASCADE, related_name="impacts"
    )
    return_period = models.IntegerField(db_index=True)

    pop_exposed = models.IntegerField(default=0)
    pop_exposed_pct = models.FloatField(default=0)
    flood_area_km2 = models.FloatField(default=0)
    area_flooded_pct = models.FloatField(default=0)
    mean_depth_m = models.FloatField(default=0)
    cropland_flooded_km2 = models.FloatField(default=0)
    extra_metrics = models.JSONField(default=dict, blank=True)

    computed_at = models.DateTimeField(auto_now=True)

    class Meta:
        unique_together = ["admin_unit", "hazard_type", "return_period"]
        ordering = ["admin_unit", "return_period"]
        indexes = [
            models.Index(fields=["hazard_type", "return_period"]),
        ]

    def __str__(self):
        return f"{self.admin_unit.gid_2} RP{self.return_period}: {self.pop_exposed} exposed"


class ForecastStation(models.Model):
    """GloFAS reporting point mapped to downstream admin2 districts."""

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    station_id = models.CharField(max_length=50, unique=True)
    name = models.CharField(max_length=200)
    river = models.CharField(max_length=200, blank=True)
    location = models.PointField(srid=4326)
    rp_thresholds = models.JSONField(
        default=dict,
        help_text='Discharge thresholds: {"10": 1200, "20": 1800, ...}',
    )
    affected_units = models.ManyToManyField(
        AdminUnit, blank=True, related_name="upstream_stations"
    )
    is_active = models.BooleanField(default=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["name"]

    def __str__(self):
        return f"{self.name} ({self.station_id})"


class ForecastRun(models.Model):
    """One GloFAS forecast retrieval + processing run."""

    STATUS_CHOICES = [
        ("pending", "Pending"),
        ("extracting", "Extracting"),
        ("processing", "Processing"),
        ("triggering", "Evaluating Triggers"),
        ("completed", "Completed"),
        ("failed", "Failed"),
    ]

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    forecast_date = models.DateField(db_index=True)
    issued_at = models.DateTimeField()
    lead_time_days = models.IntegerField(default=7)
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default="pending")
    source_file = models.CharField(max_length=500, blank=True)
    stations_processed = models.IntegerField(default=0)
    alerts_generated = models.IntegerField(default=0)
    log = models.TextField(blank=True)
    error = models.TextField(blank=True)
    started_at = models.DateTimeField(null=True, blank=True)
    completed_at = models.DateTimeField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["-forecast_date"]
        get_latest_by = "forecast_date"

    def __str__(self):
        return f"Forecast {self.forecast_date} [{self.status}]"

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


class StationForecast(models.Model):
    """Forecasted discharge + derived RP at one station for one lead time."""

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    forecast_run = models.ForeignKey(
        ForecastRun, on_delete=models.CASCADE, related_name="station_forecasts"
    )
    station = models.ForeignKey(
        ForecastStation, on_delete=models.CASCADE, related_name="forecasts"
    )
    lead_time_days = models.IntegerField()
    discharge_m3s = models.FloatField()
    discharge_max_m3s = models.FloatField(null=True, blank=True)
    discharge_min_m3s = models.FloatField(null=True, blank=True)
    derived_rp = models.IntegerField(null=True, blank=True)
    probability_above_rp = models.FloatField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        unique_together = ["forecast_run", "station", "lead_time_days"]
        ordering = ["station", "lead_time_days"]

    def __str__(self):
        rp = f"RP{self.derived_rp}" if self.derived_rp else "below"
        return f"{self.station.name} +{self.lead_time_days}d: {self.discharge_m3s:.0f} m3/s ({rp})"


class TriggerThreshold(models.Model):
    """Activation threshold for a country or district."""

    LEVEL_CHOICES = [
        ("country", "Country-level"),
        ("admin1", "Province-level"),
        ("admin2", "District-level"),
    ]
    SEVERITY_CHOICES = [
        ("watch", "Watch"),
        ("warning", "Warning"),
        ("alert", "Alert"),
    ]

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    hazard_type = models.ForeignKey(
        HazardType, on_delete=models.CASCADE, related_name="thresholds"
    )
    level = models.CharField(max_length=10, choices=LEVEL_CHOICES, default="country")
    iso3 = models.CharField(max_length=3, blank=True, db_index=True)
    admin_unit = models.ForeignKey(
        AdminUnit, null=True, blank=True, on_delete=models.CASCADE
    )
    severity = models.CharField(max_length=10, choices=SEVERITY_CHOICES)
    min_return_period = models.IntegerField()
    min_pop_exposed = models.IntegerField(default=0)
    min_probability = models.FloatField(default=0.5)
    min_lead_time_days = models.IntegerField(default=1)
    is_active = models.BooleanField(default=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["hazard_type", "severity"]

    def __str__(self):
        scope = self.iso3 or (self.admin_unit.gid_2 if self.admin_unit else "global")
        return f"{self.get_severity_display()}: RP>={self.min_return_period} for {scope}"


class Alert(models.Model):
    """An IBF alert — the core output of the system."""

    STATUS_CHOICES = [
        ("issued", "Issued"),
        ("active", "Active"),
        ("expired", "Expired"),
        ("cancelled", "Cancelled"),
    ]
    SEVERITY_CHOICES = [
        ("watch", "Watch"),
        ("warning", "Warning"),
        ("alert", "Alert"),
    ]

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    forecast_run = models.ForeignKey(
        ForecastRun, on_delete=models.CASCADE, related_name="alerts"
    )
    admin_unit = models.ForeignKey(
        AdminUnit, on_delete=models.CASCADE, related_name="alerts"
    )
    hazard_type = models.ForeignKey(
        HazardType, on_delete=models.CASCADE, related_name="alerts"
    )
    threshold = models.ForeignKey(
        TriggerThreshold, on_delete=models.SET_NULL, null=True
    )
    status = models.CharField(max_length=12, choices=STATUS_CHOICES, default="issued")
    severity = models.CharField(max_length=10, choices=SEVERITY_CHOICES)
    return_period = models.IntegerField()
    lead_time_days = models.IntegerField()
    discharge_m3s = models.FloatField()
    probability = models.FloatField()
    pop_exposed = models.IntegerField(default=0)
    pop_exposed_pct = models.FloatField(default=0)
    flood_area_km2 = models.FloatField(default=0)
    cropland_flooded_km2 = models.FloatField(default=0)
    issued_at = models.DateTimeField(default=timezone.now)
    activated_at = models.DateTimeField(null=True, blank=True)
    expires_at = models.DateTimeField(null=True, blank=True)
    cancelled_at = models.DateTimeField(null=True, blank=True)
    notes = models.TextField(blank=True)
    extra = models.JSONField(default=dict, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-issued_at", "-severity"]
        indexes = [
            models.Index(fields=["status", "severity"]),
            models.Index(fields=["admin_unit", "status"]),
        ]

    def __str__(self):
        return (
            f"[{self.severity.upper()}] {self.admin_unit.admin2_name} "
            f"RP{self.return_period} +{self.lead_time_days}d"
        )
