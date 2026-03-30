"""Thin entrypoint for optional gridded COG export."""

from __future__ import annotations

from etl.pipelines.gridded.cog import main


if __name__ == "__main__":
    main()
