from django.urls import include, path

from . import views

urlpatterns = [
    path("api/ibf/", include("ibf.api.urls")),
    path("ibf/", views.dashboard, name="ibf-dashboard"),
    path("ibf/alert/<uuid:pk>/", views.alert_detail, name="ibf-alert-detail"),
]
