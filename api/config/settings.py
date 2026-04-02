"""
Minimal Django settings for geodata-scraper.
"""

import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent

# GDAL/GEOS from rasterio bundle (for environments without system GDAL)
_rasterio_libs = BASE_DIR.parent / ".venv/lib/python3.12/site-packages/rasterio.libs"
if _rasterio_libs.exists():
    import glob as _glob
    _gdal = _glob.glob(str(_rasterio_libs / "libgdal*.so*"))
    _geos = _glob.glob(str(_rasterio_libs / "libgeos_c*.so*"))
    if _gdal and not os.environ.get("GDAL_LIBRARY_PATH"):
        os.environ["GDAL_LIBRARY_PATH"] = _gdal[0]
    if _geos and not os.environ.get("GEOS_LIBRARY_PATH"):
        os.environ["GEOS_LIBRARY_PATH"] = _geos[0]

SECRET_KEY = os.environ.get("SECRET_KEY", "dev-secret-change-in-production")
DEBUG = os.environ.get("DEBUG", "1") == "1"
ALLOWED_HOSTS = ["*"]

INSTALLED_APPS = [
    # Wagtail
    "wagtail.contrib.forms",
    "wagtail.contrib.redirects",
    "wagtail.embeds",
    "wagtail.sites",
    "wagtail.users",
    "wagtail.snippets",
    "wagtail.documents",
    "wagtail.images",
    "wagtail.search",
    "wagtail.admin",
    "wagtail",
    "taggit",
    # Django
    "django.contrib.admin",
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.sessions",
    "django.contrib.messages",
    "django.contrib.staticfiles",
    "django.contrib.gis",
    "django.contrib.postgres",
    # API
    "rest_framework",
    "rest_framework_gis",
    # Extensions
    "wagtail_modeladmin",
    # App
    "geodata",
    "ibf",
]

MIDDLEWARE = [
    "django.middleware.security.SecurityMiddleware",
    "whitenoise.middleware.WhiteNoiseMiddleware",
    "django.contrib.sessions.middleware.SessionMiddleware",
    "django.middleware.common.CommonMiddleware",
    "django.middleware.csrf.CsrfViewMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",
    "django.contrib.messages.middleware.MessageMiddleware",
    "wagtail.contrib.redirects.middleware.RedirectMiddleware",
]

ROOT_URLCONF = "config.urls"

TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "DIRS": [],
        "APP_DIRS": True,
        "OPTIONS": {
            "context_processors": [
                "django.template.context_processors.debug",
                "django.template.context_processors.request",
                "django.contrib.auth.context_processors.auth",
                "django.contrib.messages.context_processors.messages",
            ],
        },
    },
]

WSGI_APPLICATION = "config.wsgi.application"

if os.environ.get("USE_SQLITE"):
    DATABASES = {
        "default": {
            "ENGINE": "django.db.backends.sqlite3",
            "NAME": os.environ.get("DB_NAME", str(BASE_DIR / "db.sqlite3")),
        }
    }
else:
    DATABASES = {
        "default": {
            "ENGINE": "django.contrib.gis.db.backends.postgis",
            "NAME": os.environ.get("DB_NAME", "geodata"),
            "USER": os.environ.get("DB_USER", "geodata"),
            "PASSWORD": os.environ.get("DB_PASSWORD", "geodata"),
            "HOST": os.environ.get("DB_HOST", "localhost"),
            "PORT": os.environ.get("DB_PORT", "5433"),
        }
    }

STATIC_URL = "/static/"
STATIC_ROOT = BASE_DIR / "staticfiles"
STORAGES = {
    "default": {
        "BACKEND": "django.core.files.storage.FileSystemStorage",
    },
    "staticfiles": {
        "BACKEND": "whitenoise.storage.CompressedManifestStaticFilesStorage",
    },
}

DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"

# geodata-scraper config
GEODATA_SCRAPER = {
    "DB_SCHEMA_RAW": "geodata_raw",
    "DB_SCHEMA_CLEAN": "geodata",
    "STORAGE_DIR": os.environ.get("GEODATA_STORAGE", "/app/data"),
    "DOWNLOAD_WORKERS": 2,
    "DOWNLOAD_TIMEOUT": 600,
}

REST_FRAMEWORK = {
    "DEFAULT_PAGINATION_CLASS": "rest_framework.pagination.PageNumberPagination",
    "PAGE_SIZE": 50,
}

# Wagtail
WAGTAIL_SITE_NAME = "Geodata Scraper"
WAGTAILADMIN_BASE_URL = os.environ.get("WAGTAILADMIN_BASE_URL", "http://localhost:8000")
WAGTAILSEARCH_BACKENDS = {
    "default": {"BACKEND": "wagtail.search.backends.database"},
}

MEDIA_ROOT = BASE_DIR / "media"
MEDIA_URL = "/media/"
