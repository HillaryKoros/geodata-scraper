from rest_framework import serializers

from ..models import DataSource, ScrapeJob, IngestedLayer


class DataSourceSerializer(serializers.ModelSerializer):
    class Meta:
        model = DataSource
        fields = ["id", "name", "source_type", "protocol", "is_active", "created_at"]


class ScrapeJobSerializer(serializers.ModelSerializer):
    source_name = serializers.CharField(source="source.name", read_only=True)
    duration = serializers.SerializerMethodField()

    class Meta:
        model = ScrapeJob
        fields = [
            "id",
            "source_name",
            "status",
            "region",
            "countries",
            "total_files",
            "downloaded_files",
            "loaded_tables",
            "failed_files",
            "bytes_downloaded",
            "started_at",
            "completed_at",
            "duration",
        ]

    def get_duration(self, obj):
        d = obj.duration
        if d:
            return str(d)
        return None


class IngestedLayerSerializer(serializers.ModelSerializer):
    class Meta:
        model = IngestedLayer
        fields = [
            "id",
            "name",
            "iso3",
            "admin_level",
            "db_schema",
            "db_table",
            "geom_type",
            "srid",
            "feature_count",
            "properties",
            "source_url",
            "source_format",
            "created_at",
        ]


class BoundaryGeoJSONSerializer(serializers.Serializer):
    """Serializes raw PostGIS query results as GeoJSON."""

    type = serializers.CharField(default="FeatureCollection")
    features = serializers.ListField()
