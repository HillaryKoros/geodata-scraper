from django.urls import path, include

app_name = "geodata_scraper"

urlpatterns = [
    path("api/geodata/", include("geodata_scraper.api.urls")),
]
