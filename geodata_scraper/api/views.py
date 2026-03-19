"""
REST API views for geodata_scraper.

Endpoints:
    /api/geodata/sources/            — list data sources
    /api/geodata/jobs/               — list scrape jobs
    /api/geodata/layers/             — list ingested layers
    /api/geodata/layers/{id}/geojson — GeoJSON for a layer
    /api/geodata/boundaries/{iso3}/  — boundaries by country
    /api/geodata/boundaries/{iso3}/{level}/ — specific admin level GeoJSON
"""

from django.db import connection
from django.http import JsonResponse
from rest_framework import viewsets, status
from rest_framework.decorators import action, api_view
from rest_framework.response import Response

from ..models import DataSource, ScrapeJob, IngestedLayer
from .serializers import DataSourceSerializer, ScrapeJobSerializer, IngestedLayerSerializer


class DataSourceViewSet(viewsets.ReadOnlyModelViewSet):
    queryset = DataSource.objects.all()
    serializer_class = DataSourceSerializer


class ScrapeJobViewSet(viewsets.ReadOnlyModelViewSet):
    queryset = ScrapeJob.objects.select_related("source").all()
    serializer_class = ScrapeJobSerializer


class IngestedLayerViewSet(viewsets.ReadOnlyModelViewSet):
    queryset = IngestedLayer.objects.all()
    serializer_class = IngestedLayerSerializer
    filterset_fields = ["iso3", "admin_level", "source", "db_schema"]

    @action(detail=True, methods=["get"])
    def geojson(self, request, pk=None):
        """Return GeoJSON FeatureCollection for this layer."""
        layer = self.get_object()
        return _layer_to_geojson(layer, request)


@api_view(["GET"])
def boundary_by_country(request, iso3):
    """List available admin levels for a country."""
    iso3 = iso3.upper()
    layers = IngestedLayer.objects.filter(iso3=iso3).order_by("admin_level")
    serializer = IngestedLayerSerializer(layers, many=True)
    return Response({
        "country": iso3,
        "admin_levels": serializer.data,
    })


@api_view(["GET"])
def boundary_geojson(request, iso3, level):
    """Return GeoJSON for a specific country + admin level."""
    iso3 = iso3.upper()
    try:
        layer = IngestedLayer.objects.get(iso3=iso3, admin_level=level)
    except IngestedLayer.DoesNotExist:
        return Response(
            {"error": f"No data for {iso3} admin level {level}"},
            status=status.HTTP_404_NOT_FOUND,
        )
    return _layer_to_geojson(layer, request)


@api_view(["GET"])
def countries_list(request):
    """List all countries with ingested boundary data."""
    countries = (
        IngestedLayer.objects
        .values("iso3")
        .distinct()
        .order_by("iso3")
    )
    result = []
    for c in countries:
        iso3 = c["iso3"]
        levels = list(
            IngestedLayer.objects
            .filter(iso3=iso3)
            .values_list("admin_level", flat=True)
            .order_by("admin_level")
        )
        total_features = sum(
            IngestedLayer.objects
            .filter(iso3=iso3)
            .values_list("feature_count", flat=True)
        )
        result.append({
            "iso3": iso3,
            "admin_levels": levels,
            "total_features": total_features,
        })

    return Response({"countries": result, "count": len(result)})


def _layer_to_geojson(layer, request):
    """Query PostGIS and return GeoJSON FeatureCollection."""
    simplify = request.query_params.get("simplify")
    limit = request.query_params.get("limit")
    bbox = request.query_params.get("bbox")

    geom_expr = f'"{layer.geom_column}"'
    if simplify:
        geom_expr = f"ST_Simplify({geom_expr}, {float(simplify)})"

    # Build query
    where_clauses = []
    params = []

    if bbox:
        parts = [float(x) for x in bbox.split(",")]
        if len(parts) == 4:
            where_clauses.append(
                f'"{layer.geom_column}" && ST_MakeEnvelope(%s, %s, %s, %s, 4326)'
            )
            params.extend(parts)

    where_sql = ""
    if where_clauses:
        where_sql = "WHERE " + " AND ".join(where_clauses)

    limit_sql = ""
    if limit:
        limit_sql = f"LIMIT {int(limit)}"

    # Exclude geometry columns from properties
    prop_cols = [c for c in layer.properties if c not in ("geom", "ogc_fid", layer.geom_column)]
    props_sql = ", ".join(f'"{c}"' for c in prop_cols) if prop_cols else "'{}'"

    sql = f"""
        SELECT json_build_object(
            'type', 'FeatureCollection',
            'features', COALESCE(json_agg(
                json_build_object(
                    'type', 'Feature',
                    'geometry', ST_AsGeoJSON({geom_expr})::json,
                    'properties', json_build_object({
                        ', '.join(f"'{c}', \"{c}\"" for c in prop_cols)
                    })
                )
            ), '[]'::json)
        )
        FROM "{layer.db_schema}"."{layer.db_table}"
        {where_sql}
        {limit_sql}
    """

    with connection.cursor() as cur:
        cur.execute(sql, params)
        row = cur.fetchone()

    if row and row[0]:
        return JsonResponse(row[0], safe=False)

    return JsonResponse({"type": "FeatureCollection", "features": []})
