"""Load pre-computed impact table into Django DB.

Usage:
    python manage.py ibf_load_impact
    python manage.py ibf_load_impact --csv data/processed/ibf/ibf_impact_table.csv
"""

from pathlib import Path

import pandas as pd
from django.core.management.base import BaseCommand

from ibf.models import AdminUnit, HazardType, ImpactEstimate


class Command(BaseCommand):
    help = "Load IBF impact table CSV/Parquet into the database"

    def add_arguments(self, parser):
        parser.add_argument(
            "--csv",
            default="data/processed/ibf/ibf_impact_table.csv",
            help="Path to impact table CSV",
        )
        parser.add_argument(
            "--clear",
            action="store_true",
            help="Clear existing impact estimates before loading",
        )

    def handle(self, *args, **options):
        csv_path = Path(options["csv"])

        # Try parquet first, fall back to CSV
        parquet_path = csv_path.with_suffix(".parquet")
        if parquet_path.exists():
            df = pd.read_parquet(parquet_path)
            self.stdout.write(f"Loaded {len(df)} rows from {parquet_path}")
        elif csv_path.exists():
            df = pd.read_csv(csv_path)
            self.stdout.write(f"Loaded {len(df)} rows from {csv_path}")
        else:
            self.stderr.write(f"File not found: {csv_path}")
            return

        # Ensure flood hazard type exists
        hazard, _ = HazardType.objects.get_or_create(
            code="flood",
            defaults={
                "name": "Riverine Flood",
                "description": "JRC CEMS-GloFAS flood hazard maps",
                "return_periods": [10, 20, 50, 75, 100],
            },
        )

        # Create/update admin units
        gid2_to_unit = {}
        unique_districts = df.drop_duplicates("gid_2")
        self.stdout.write(f"Syncing {len(unique_districts)} admin units...")

        for _, row in unique_districts.iterrows():
            unit, _ = AdminUnit.objects.update_or_create(
                gid_2=row["gid_2"],
                defaults={
                    "country": row["country"],
                    "iso3": row.get("gid_2", "")[:3],
                    "admin1_name": row["admin1"],
                    "admin2_name": row["admin2"],
                    "pop_total": int(row["pop_total"]),
                },
            )
            gid2_to_unit[row["gid_2"]] = unit

        # Clear existing if requested
        if options["clear"]:
            deleted, _ = ImpactEstimate.objects.filter(hazard_type=hazard).delete()
            self.stdout.write(f"Cleared {deleted} existing estimates")

        # Bulk create impact estimates
        self.stdout.write("Loading impact estimates...")
        estimates = []
        for _, row in df.iterrows():
            unit = gid2_to_unit.get(row["gid_2"])
            if not unit:
                continue
            estimates.append(
                ImpactEstimate(
                    admin_unit=unit,
                    hazard_type=hazard,
                    return_period=int(row["rp"]),
                    pop_exposed=int(row["pop_exposed"]),
                    pop_exposed_pct=float(row["pop_exposed_pct"]),
                    flood_area_km2=float(row["flood_area_km2"]),
                    area_flooded_pct=float(row["area_flooded_pct"]),
                    mean_depth_m=float(row["mean_depth_m"]),
                    cropland_flooded_km2=float(row.get("cropland_flooded_km2", 0)),
                )
            )

        ImpactEstimate.objects.bulk_create(
            estimates,
            update_conflicts=True,
            unique_fields=["admin_unit", "hazard_type", "return_period"],
            update_fields=[
                "pop_exposed", "pop_exposed_pct", "flood_area_km2",
                "area_flooded_pct", "mean_depth_m", "cropland_flooded_km2",
            ],
        )
        self.stdout.write(self.style.SUCCESS(f"Loaded {len(estimates)} impact estimates"))
