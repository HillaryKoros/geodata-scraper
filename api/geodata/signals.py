"""
Post-ingest signals — notify when data lands in PostGIS.
"""

import django.dispatch

# Fired after a layer is successfully loaded into PostGIS
layer_ingested = django.dispatch.Signal()  # sender=IngestedLayer

# Fired after all layers in a job are loaded
job_completed = django.dispatch.Signal()  # sender=ScrapeJob

# Fired after transforms are applied
layer_transformed = django.dispatch.Signal()  # sender=IngestedLayer
