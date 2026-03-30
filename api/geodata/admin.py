"""
Auto-registered Django admin for geodata models.

Just add 'geodata' to INSTALLED_APPS — admin is auto-configured.
"""

from django.contrib import admin
from django.contrib.gis.admin import GISModelAdmin
from django.utils.html import format_html

from .models import DataSource, ScrapeJob, IngestedLayer


@admin.register(DataSource)
class DataSourceAdmin(admin.ModelAdmin):
    list_display = ["name", "source_type", "protocol", "is_active", "updated_at"]
    list_filter = ["source_type", "protocol", "is_active"]
    search_fields = ["name", "base_url"]
    readonly_fields = ["id", "created_at", "updated_at"]


@admin.register(ScrapeJob)
class ScrapeJobAdmin(admin.ModelAdmin):
    list_display = [
        "short_id",
        "source",
        "status_badge",
        "region",
        "downloaded_files",
        "loaded_tables",
        "failed_files",
        "size_display",
        "duration_display",
        "started_at",
    ]
    list_filter = ["status", "source"]
    readonly_fields = [
        "id",
        "started_at",
        "completed_at",
        "log",
        "error",
        "total_files",
        "downloaded_files",
        "loaded_tables",
        "failed_files",
        "bytes_downloaded",
    ]

    def short_id(self, obj):
        return obj.id.hex[:8]

    short_id.short_description = "Job ID"

    def status_badge(self, obj):
        colors = {
            "pending": "#999",
            "extracting": "#f0ad4e",
            "loading": "#5bc0de",
            "transforming": "#0275d8",
            "completed": "#5cb85c",
            "failed": "#d9534f",
        }
        color = colors.get(obj.status, "#999")
        return format_html(
            '<span style="color:{}; font-weight:bold;">{}</span>',
            color,
            obj.get_status_display(),
        )

    status_badge.short_description = "Status"

    def size_display(self, obj):
        n = obj.bytes_downloaded
        for u in ("B", "KB", "MB", "GB"):
            if abs(n) < 1024:
                return f"{n:.1f} {u}"
            n /= 1024
        return f"{n:.1f} TB"

    size_display.short_description = "Downloaded"

    def duration_display(self, obj):
        d = obj.duration
        if d:
            secs = int(d.total_seconds())
            if secs < 60:
                return f"{secs}s"
            return f"{secs // 60}m {secs % 60}s"
        return "—"

    duration_display.short_description = "Duration"


@admin.register(IngestedLayer)
class IngestedLayerAdmin(GISModelAdmin):
    list_display = [
        "name",
        "iso3",
        "admin_level",
        "db_schema",
        "db_table",
        "feature_count",
        "geom_type",
        "source_format",
        "created_at",
    ]
    list_filter = ["iso3", "admin_level", "geom_type", "source", "db_schema"]
    search_fields = ["name", "iso3", "db_table"]
    readonly_fields = [
        "id",
        "feature_count",
        "properties",
        "bbox",
        "file_size",
        "created_at",
        "updated_at",
    ]
