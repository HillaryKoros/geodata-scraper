"""
Bridge Django management command to the standalone ETL package.

Usage:
    python manage.py run_etl --list
    python manage.py run_etl --only gadm,dem
    python manage.py run_etl --skip buildings --parallel 4
"""

from django.core.management.base import BaseCommand


class Command(BaseCommand):
    help = "Run the geodata-etl pipeline from Django"

    def add_arguments(self, parser):
        parser.add_argument("--list", action="store_true", help="List available ETL steps")
        parser.add_argument("--only", type=str, help="Run only these steps (comma-separated)")
        parser.add_argument("--skip", type=str, help="Skip these steps (comma-separated)")
        parser.add_argument("--parallel", type=int, default=3, help="Parallel workers per phase")

    def handle(self, *args, **options):
        from etl.orchestrator import main as etl_main

        argv = []
        if options["list"]:
            argv.append("--list")
        if options["only"]:
            argv.extend(["--only", options["only"]])
        if options["skip"]:
            argv.extend(["--skip", options["skip"]])
        if options["parallel"]:
            argv.extend(["--parallel", str(options["parallel"])])

        etl_main(argv)
