from rest_framework import serializers
from rest_framework_gis.serializers import GeoFeatureModelSerializer

from ibf.models import (
    AdminUnit,
    Alert,
    ForecastRun,
    ForecastStation,
    HazardType,
    ImpactEstimate,
    StationForecast,
    TriggerThreshold,
)


class HazardTypeSerializer(serializers.ModelSerializer):
    class Meta:
        model = HazardType
        fields = ["id", "code", "name", "description", "return_periods", "is_active"]


class AdminUnitSerializer(serializers.ModelSerializer):
    class Meta:
        model = AdminUnit
        fields = [
            "id", "gid_2", "country", "iso3", "admin1_name", "admin2_name",
            "pop_total", "area_km2",
        ]


class ImpactEstimateSerializer(serializers.ModelSerializer):
    gid_2 = serializers.CharField(source="admin_unit.gid_2", read_only=True)
    country = serializers.CharField(source="admin_unit.country", read_only=True)
    admin1 = serializers.CharField(source="admin_unit.admin1_name", read_only=True)
    admin2 = serializers.CharField(source="admin_unit.admin2_name", read_only=True)

    class Meta:
        model = ImpactEstimate
        fields = [
            "id", "gid_2", "country", "admin1", "admin2",
            "return_period", "pop_exposed", "pop_exposed_pct",
            "flood_area_km2", "area_flooded_pct", "mean_depth_m",
            "cropland_flooded_km2", "extra_metrics", "computed_at",
        ]


class ForecastStationSerializer(serializers.ModelSerializer):
    class Meta:
        model = ForecastStation
        fields = [
            "id", "station_id", "name", "river", "location",
            "rp_thresholds", "is_active",
        ]


class StationForecastSerializer(serializers.ModelSerializer):
    station_name = serializers.CharField(source="station.name", read_only=True)

    class Meta:
        model = StationForecast
        fields = [
            "id", "station", "station_name", "lead_time_days",
            "discharge_m3s", "discharge_max_m3s", "discharge_min_m3s",
            "derived_rp", "probability_above_rp",
        ]


class ForecastRunSerializer(serializers.ModelSerializer):
    duration = serializers.SerializerMethodField()

    class Meta:
        model = ForecastRun
        fields = [
            "id", "forecast_date", "issued_at", "lead_time_days", "status",
            "stations_processed", "alerts_generated",
            "started_at", "completed_at", "duration",
        ]

    def get_duration(self, obj):
        d = obj.duration
        return str(d) if d else None


class ForecastRunDetailSerializer(ForecastRunSerializer):
    station_forecasts = StationForecastSerializer(many=True, read_only=True)

    class Meta(ForecastRunSerializer.Meta):
        fields = ForecastRunSerializer.Meta.fields + ["station_forecasts", "log"]


class TriggerThresholdSerializer(serializers.ModelSerializer):
    hazard_code = serializers.CharField(source="hazard_type.code", read_only=True)

    class Meta:
        model = TriggerThreshold
        fields = [
            "id", "hazard_type", "hazard_code", "level", "iso3",
            "admin_unit", "severity", "min_return_period",
            "min_pop_exposed", "min_probability", "min_lead_time_days",
            "is_active",
        ]


class AlertSerializer(serializers.ModelSerializer):
    country = serializers.CharField(source="admin_unit.country", read_only=True)
    admin1 = serializers.CharField(source="admin_unit.admin1_name", read_only=True)
    admin2 = serializers.CharField(source="admin_unit.admin2_name", read_only=True)
    gid_2 = serializers.CharField(source="admin_unit.gid_2", read_only=True)
    hazard_code = serializers.CharField(source="hazard_type.code", read_only=True)

    class Meta:
        model = Alert
        fields = [
            "id", "forecast_run", "gid_2", "country", "admin1", "admin2",
            "hazard_code", "status", "severity",
            "return_period", "lead_time_days", "discharge_m3s", "probability",
            "pop_exposed", "pop_exposed_pct", "flood_area_km2", "cropland_flooded_km2",
            "issued_at", "activated_at", "expires_at", "notes",
        ]
        read_only_fields = [
            "id", "forecast_run", "gid_2", "country", "admin1", "admin2",
            "hazard_code", "return_period", "lead_time_days", "discharge_m3s",
            "probability", "pop_exposed", "pop_exposed_pct", "flood_area_km2",
            "cropland_flooded_km2", "issued_at",
        ]


class AlertGeoSerializer(GeoFeatureModelSerializer):
    country = serializers.CharField(source="admin_unit.country", read_only=True)
    admin1 = serializers.CharField(source="admin_unit.admin1_name", read_only=True)
    admin2 = serializers.CharField(source="admin_unit.admin2_name", read_only=True)
    gid_2 = serializers.CharField(source="admin_unit.gid_2", read_only=True)
    geometry = serializers.SerializerMethodField()

    class Meta:
        model = Alert
        geo_field = "geometry"
        fields = [
            "id", "gid_2", "country", "admin1", "admin2",
            "severity", "status", "return_period", "lead_time_days",
            "pop_exposed", "pop_exposed_pct", "flood_area_km2",
            "issued_at",
        ]

    def get_geometry(self, obj):
        if obj.admin_unit.geom:
            return obj.admin_unit.geom
        return None
