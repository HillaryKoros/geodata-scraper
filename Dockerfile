FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV DJANGO_SETTINGS_MODULE=config.settings

# System deps: GDAL, ogr2ogr, PostGIS client
RUN apt-get update && apt-get install -y --no-install-recommends \
    gdal-bin \
    libgdal-dev \
    libpq-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Python deps
COPY pyproject.toml .
RUN pip install --no-cache-dir \
    django>=4.2 \
    djangorestframework>=3.14 \
    djangorestframework-gis>=1.0 \
    psycopg2-binary>=2.9 \
    httpx>=0.27 \
    geopandas>=0.14 \
    shapely>=2.0 \
    fiona>=1.9 \
    wagtail>=6.0 \
    wagtail-modeladmin>=2.0 \
    whitenoise \
    gunicorn

# Copy app
COPY . .

# Collect static, run migrations on start
COPY docker-entrypoint.sh /docker-entrypoint.sh
RUN chmod +x /docker-entrypoint.sh

EXPOSE 8000

ENTRYPOINT ["/docker-entrypoint.sh"]
CMD ["gunicorn", "config.wsgi:application", "--bind", "0.0.0.0:8000", "--workers", "2"]
