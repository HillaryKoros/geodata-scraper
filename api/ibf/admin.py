from django.contrib import admin

from .models import (
    AdminUnit,
    Alert,
    ForecastRun,
    ForecastStation,
    HazardType,
    ImpactEstimate,
    StationForecast,
    TriggerThreshold,
)


@admin.register(HazardType)
class HazardTypeAdmin(admin.ModelAdmin):
    list_display = ["code", "name", "is_active", "created_at"]
    list_filter = ["is_active"]


@admin.register(AdminUnit)
class AdminUnitAdmin(admin.ModelAdmin):
    list_display = ["gid_2", "admin2_name", "admin1_name", "country", "iso3", "pop_total"]
    list_filter = ["iso3"]
    search_fields = ["admin2_name", "admin1_name", "gid_2"]


@admin.register(ImpactEstimate)
class ImpactEstimateAdmin(admin.ModelAdmin):
    list_display = [
        "admin_unit", "hazard_type", "return_period",
        "pop_exposed", "pop_exposed_pct", "flood_area_km2",
    ]
    list_filter = ["hazard_type", "return_period"]
    search_fields = ["admin_unit__gid_2", "admin_unit__admin2_name"]


@admin.register(ForecastStation)
class ForecastStationAdmin(admin.ModelAdmin):
    list_display = ["station_id", "name", "river", "is_active"]
    list_filter = ["is_active"]


@admin.register(ForecastRun)
class ForecastRunAdmin(admin.ModelAdmin):
    list_display = [
        "forecast_date", "status", "stations_processed",
        "alerts_generated", "started_at", "completed_at",
    ]
    list_filter = ["status"]
    readonly_fields = ["id", "started_at", "completed_at", "log", "error"]


@admin.register(StationForecast)
class StationForecastAdmin(admin.ModelAdmin):
    list_display = [
        "station", "forecast_run", "lead_time_days",
        "discharge_m3s", "derived_rp", "probability_above_rp",
    ]
    list_filter = ["derived_rp"]


@admin.register(TriggerThreshold)
class TriggerThresholdAdmin(admin.ModelAdmin):
    list_display = [
        "hazard_type", "severity", "level", "iso3",
        "min_return_period", "min_pop_exposed", "is_active",
    ]
    list_filter = ["severity", "hazard_type", "is_active"]


@admin.register(Alert)
class AlertAdmin(admin.ModelAdmin):
    list_display = [
        "admin_unit", "severity", "status", "return_period",
        "lead_time_days", "pop_exposed", "issued_at",
    ]
    list_filter = ["status", "severity", "hazard_type"]
    search_fields = ["admin_unit__admin2_name", "admin_unit__gid_2"]
    readonly_fields = ["id", "created_at", "updated_at"]
