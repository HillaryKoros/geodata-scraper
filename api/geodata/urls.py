from django.urls import path, include

from . import views

app_name = "geodata"

urlpatterns = [
    path("api/geodata/", include("geodata.api.urls")),
    path("map/layer/<uuid:pk>/", views.mapviewer, name="mapviewer"),
    path("map/gha/<int:level>/", views.gha_mapviewer, name="gha-mapviewer"),
]
