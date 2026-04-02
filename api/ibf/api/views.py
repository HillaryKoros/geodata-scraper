from django.db.models import Count, Sum
from rest_framework import viewsets
from rest_framework.decorators import api_view
from rest_framework.response import Response

from ibf.models import (
    AdminUnit,
    Alert,
    ForecastRun,
    ForecastStation,
    HazardType,
    ImpactEstimate,
    TriggerThreshold,
)

from .serializers import (
    AdminUnitSerializer,
    AlertGeoSerializer,
    AlertSerializer,
    ForecastRunDetailSerializer,
    ForecastRunSerializer,
    ForecastStationSerializer,
    HazardTypeSerializer,
    ImpactEstimateSerializer,
    TriggerThresholdSerializer,
)


class HazardTypeViewSet(viewsets.ReadOnlyModelViewSet):
    queryset = HazardType.objects.filter(is_active=True)
    serializer_class = HazardTypeSerializer


class AdminUnitViewSet(viewsets.ReadOnlyModelViewSet):
    queryset = AdminUnit.objects.all()
    serializer_class = AdminUnitSerializer
    filterset_fields = ["iso3"]


class ImpactEstimateViewSet(viewsets.ReadOnlyModelViewSet):
    queryset = ImpactEstimate.objects.select_related("admin_unit", "hazard_type")
    serializer_class = ImpactEstimateSerializer
    filterset_fields = ["hazard_type__code", "return_period", "admin_unit__iso3"]


class ForecastStationViewSet(viewsets.ReadOnlyModelViewSet):
    queryset = ForecastStation.objects.filter(is_active=True)
    serializer_class = ForecastStationSerializer


class ForecastRunViewSet(viewsets.ReadOnlyModelViewSet):
    queryset = ForecastRun.objects.all()

    def get_serializer_class(self):
        if self.action == "retrieve":
            return ForecastRunDetailSerializer
        return ForecastRunSerializer


class TriggerThresholdViewSet(viewsets.ModelViewSet):
    queryset = TriggerThreshold.objects.select_related("hazard_type")
    serializer_class = TriggerThresholdSerializer
    filterset_fields = ["hazard_type__code", "severity", "is_active"]


class AlertViewSet(viewsets.ModelViewSet):
    queryset = Alert.objects.select_related(
        "admin_unit", "hazard_type", "forecast_run"
    )
    serializer_class = AlertSerializer
    filterset_fields = ["status", "severity", "hazard_type__code", "admin_unit__iso3"]
    http_method_names = ["get", "patch", "head", "options"]


@api_view(["GET"])
def alerts_active_geojson(request):
    """GeoJSON of all active/issued alerts with admin2 geometries."""
    alerts = Alert.objects.filter(
        status__in=["issued", "active"]
    ).select_related("admin_unit", "hazard_type")
    serializer = AlertGeoSerializer(alerts, many=True)
    return Response(serializer.data)


@api_view(["GET"])
def alerts_summary(request):
    """Summary counts by severity, country, and total pop exposed."""
    active = Alert.objects.filter(status__in=["issued", "active"])

    by_severity = dict(
        active.values_list("severity").annotate(Count("id")).values_list("severity", "id__count")
    )
    by_country = list(
        active.values("admin_unit__country")
        .annotate(
            count=Count("id"),
            total_pop_exposed=Sum("pop_exposed"),
        )
        .order_by("-total_pop_exposed")
    )

    return Response({
        "total_alerts": active.count(),
        "total_pop_exposed": active.aggregate(total=Sum("pop_exposed"))["total"] or 0,
        "by_severity": by_severity,
        "by_country": [
            {
                "country": r["admin_unit__country"],
                "alerts": r["count"],
                "pop_exposed": r["total_pop_exposed"],
            }
            for r in by_country
        ],
    })


@api_view(["GET"])
def impact_by_district(request, gid_2):
    """Impact profile for one district across all RPs."""
    impacts = ImpactEstimate.objects.filter(
        admin_unit__gid_2=gid_2
    ).select_related("admin_unit", "hazard_type").order_by("return_period")

    if not impacts.exists():
        return Response({"detail": "District not found"}, status=404)

    unit = impacts.first().admin_unit
    serializer = ImpactEstimateSerializer(impacts, many=True)
    return Response({
        "gid_2": gid_2,
        "country": unit.country,
        "admin1": unit.admin1_name,
        "admin2": unit.admin2_name,
        "pop_total": unit.pop_total,
        "impacts": serializer.data,
    })


@api_view(["GET"])
def dashboard_data(request):
    """Combined payload for dashboard: active alerts + summary + latest forecast."""
    active_alerts = Alert.objects.filter(
        status__in=["issued", "active"]
    ).select_related("admin_unit", "hazard_type")[:500]

    latest_run = ForecastRun.objects.filter(status="completed").first()

    alert_serializer = AlertSerializer(active_alerts, many=True)

    by_severity = {}
    total_pop = 0
    for a in active_alerts:
        by_severity[a.severity] = by_severity.get(a.severity, 0) + 1
        total_pop += a.pop_exposed

    return Response({
        "alerts": alert_serializer.data,
        "summary": {
            "total_alerts": len(active_alerts),
            "total_pop_exposed": total_pop,
            "by_severity": by_severity,
        },
        "latest_forecast": {
            "date": latest_run.forecast_date if latest_run else None,
            "status": latest_run.status if latest_run else None,
        },
    })
