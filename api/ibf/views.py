from django.shortcuts import get_object_or_404, render

from .models import Alert


def dashboard(request):
    """IBF dashboard with Leaflet map."""
    return render(request, "ibf/dashboard.html")


def alert_detail(request, pk):
    """Single alert detail page."""
    alert = get_object_or_404(
        Alert.objects.select_related("admin_unit", "hazard_type", "forecast_run"),
        pk=pk,
    )
    return render(request, "ibf/alert_detail.html", {"alert": alert})
