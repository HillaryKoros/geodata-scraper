from django.urls import path, include
from rest_framework.routers import DefaultRouter

from . import views

router = DefaultRouter()
router.register("sources", views.DataSourceViewSet, basename="datasource")
router.register("jobs", views.ScrapeJobViewSet, basename="scrapejob")
router.register("layers", views.IngestedLayerViewSet, basename="ingestedlayer")

urlpatterns = [
    path("", include(router.urls)),
    path("countries/", views.countries_list, name="geodata-countries"),
    path(
        "boundaries/<str:iso3>/",
        views.boundary_by_country,
        name="geodata-boundary-country",
    ),
    path(
        "boundaries/<str:iso3>/<int:level>/",
        views.boundary_geojson,
        name="geodata-boundary-geojson",
    ),
    path(
        "gha/admin/<int:level>/",
        views.gha_admin_geojson,
        name="gha-admin-geojson",
    ),
    path(
        "gha/baseline/",
        views.gha_baseline_geojson,
        name="gha-baseline-geojson",
    ),
]
