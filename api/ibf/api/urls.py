from django.urls import path
from rest_framework.routers import DefaultRouter

from . import views

router = DefaultRouter()
router.register("hazards", views.HazardTypeViewSet, basename="hazard")
router.register("admin-units", views.AdminUnitViewSet, basename="admin-unit")
router.register("impacts", views.ImpactEstimateViewSet, basename="impact")
router.register("stations", views.ForecastStationViewSet, basename="station")
router.register("forecast-runs", views.ForecastRunViewSet, basename="forecast-run")
router.register("thresholds", views.TriggerThresholdViewSet, basename="threshold")
router.register("alerts", views.AlertViewSet, basename="alert")

urlpatterns = router.urls + [
    path("alerts/active/geojson/", views.alerts_active_geojson, name="alerts-geojson"),
    path("alerts/summary/", views.alerts_summary, name="alerts-summary"),
    path("impacts/<str:gid_2>/", views.impact_by_district, name="impact-district"),
    path("dashboard/data/", views.dashboard_data, name="dashboard-data"),
]
