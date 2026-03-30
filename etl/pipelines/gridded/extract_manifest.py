"""CLI entrypoint for gridded manifest generation."""

from __future__ import annotations

import argparse
from pathlib import Path

from etl.pipelines.gridded.common import DEFAULT_WORKERS, source_dir
from etl.pipelines.gridded.manifest import build_manifest


def main(argv: list[str] | None = None) -> None:
    """Generate the gridded file manifest."""
    parser = argparse.ArgumentParser(description="Build a manifest for gridded .nc.gz files")
    parser.add_argument(
        "--source-dir",
        type=Path,
        default=None,
        help="Directory containing hmc.output-grid.*.nc.gz",
    )
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS, help="Parallel validation workers")
    parser.add_argument(
        "--output-subdir",
        default=None,
        help="Processed output subdirectory under data/processed",
    )
    parser.add_argument("--limit", type=int, default=None, help="Limit number of files for a quick run")
    args = parser.parse_args(argv)

    df, summary = build_manifest(
        source_root=args.source_dir or source_dir(),
        workers=args.workers,
        limit=args.limit,
        output_subdir=args.output_subdir,
    )

    print(f"manifest rows: {len(df)}")
    print(f"source: {summary['source_dir']}")
    print(f"output: {summary['output_dir']}")
    print(f"ok files: {summary['ok_files']}, invalid files: {summary['invalid_files']}")


if __name__ == "__main__":
    main()
