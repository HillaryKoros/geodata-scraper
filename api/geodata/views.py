from django.shortcuts import get_object_or_404, render

from .models import IngestedLayer


def mapviewer(request, pk):
    """Map viewer for an ingested layer — renders GeoJSON on a Leaflet map."""
    layer = get_object_or_404(IngestedLayer, pk=pk)
    geojson_url = f"/api/geodata/layers/{layer.pk}/geojson/?simplify=0.01"

    return render(
        request,
        "geodata/mapviewer.html",
        {
            "layer_name": layer.name,
            "schema": layer.db_schema,
            "table": layer.db_table,
            "feature_count": layer.feature_count,
            "geom_type": layer.geom_type,
            "geojson_url": geojson_url,
        },
    )


def gha_mapviewer(request, level):
    """Map viewer for GHA merged admin levels."""
    geojson_url = f"/api/geodata/gha/admin/{level}/?simplify=1"

    return render(
        request,
        "geodata/mapviewer.html",
        {
            "layer_name": f"GHA Admin Level {level}",
            "schema": "gha",
            "table": f"admin{level}",
            "feature_count": "—",
            "geom_type": "MultiPolygon",
            "geojson_url": geojson_url,
        },
    )
