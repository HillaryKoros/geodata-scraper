"""
Wagtail admin registration for geodata_scraper models.

Follows the pattern from eafw_cms geomanager — uses wagtail_modeladmin
for non-Page models with custom display, filtering, and grouping.
"""

from wagtail import hooks
from wagtail_modeladmin.options import ModelAdmin, ModelAdminGroup, modeladmin_register

from .models import DataSource, ScrapeJob, IngestedLayer


class DataSourceAdmin(ModelAdmin):
    model = DataSource
    menu_label = "Data Sources"
    menu_icon = "database"
    menu_order = 100
    list_display = ["name", "source_type", "protocol", "is_active", "updated_at"]
    list_filter = ["source_type", "protocol", "is_active"]
    search_fields = ["name", "base_url"]
    inspect_view_enabled = True


class ScrapeJobAdmin(ModelAdmin):
    model = ScrapeJob
    menu_label = "Scrape Jobs"
    menu_icon = "download"
    menu_order = 200
    list_display = [
        "short_id",
        "source",
        "status",
        "region",
        "downloaded_files",
        "loaded_tables",
        "failed_files",
        "started_at",
    ]
    list_filter = ["status", "source"]
    inspect_view_enabled = True

    def short_id(self, obj):
        return obj.id.hex[:8]

    short_id.short_description = "Job ID"


class IngestedLayerAdmin(ModelAdmin):
    model = IngestedLayer
    menu_label = "Layers"
    menu_icon = "layer-group"
    menu_order = 300
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
    inspect_view_enabled = True


class GeodataAdminGroup(ModelAdminGroup):
    menu_label = "Geodata"
    menu_icon = "globe"
    menu_order = 200
    items = (DataSourceAdmin, ScrapeJobAdmin, IngestedLayerAdmin)


modeladmin_register(GeodataAdminGroup)


@hooks.register("register_icons")
def register_icons(icons):
    return icons + [
        "wagtailfontawesomesvg/solid/database.svg",
        "wagtailfontawesomesvg/solid/layer-group.svg",
        "wagtailfontawesomesvg/solid/globe.svg",
        "wagtailfontawesomesvg/solid/download.svg",
    ]
