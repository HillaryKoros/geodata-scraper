"""Thin entrypoint for the gridded GeoZarr writer."""

from __future__ import annotations

from etl.pipelines.gridded.zarr import main


if __name__ == "__main__":
    main()
