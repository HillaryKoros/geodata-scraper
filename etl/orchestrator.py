"""ETL Orchestrator — runs all pipelines in phased parallel execution.

Usage:
    python etl.py                          # run all
    python etl.py --only gadm,dem          # run specific steps
    python etl.py --skip buildings         # skip specific steps
    python etl.py --email you@example.com  # send log on completion
    python etl.py --list                   # show available steps

Phases enforce dependencies: phase N+1 waits for phase N to complete.
Within a phase, steps run in parallel.
Logs: logs/etl_YYYYMMDD_HHMMSS.log
"""

import os
import sys
import time
import logging
import smtplib
import argparse
import importlib
import traceback
from datetime import datetime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv

load_dotenv()

LOG_DIR = Path(__file__).parent / "logs"
LOG_DIR.mkdir(exist_ok=True)

# ---------------------------------------------------------------------------
# Step registry: name → (module_path, description)
# ---------------------------------------------------------------------------
STEPS = {
    "gadm":          ("etl.pipelines.gadm.extract",             "GADM admin boundaries (GEE)"),
    "gha":           ("etl.pipelines.gadm.gha_boundary",        "GHA dissolved boundary"),
    "jrc-flood":     ("etl.pipelines.jrc.extract_flood",        "JRC flood hazard tiles (HTTP)"),
    "jrc-water":     ("etl.pipelines.jrc.extract_water",        "JRC surface water (GEE)"),
    "hydrobasins":   ("etl.pipelines.hydrosheds.extract_basins","HydroBASINS 12 levels"),
    "hydrorivers":   ("etl.pipelines.hydrosheds.extract_rivers","HydroRIVERS Africa"),
    "osm-water":     ("etl.pipelines.osm.extract_water",        "OSM waterways + water bodies"),
    "osm-roads":     ("etl.pipelines.osm.extract_roads",        "OSM road network"),
    "dem":           ("etl.pipelines.dem.extract",               "Copernicus DEM 90m"),
    "buildings":     ("etl.pipelines.buildings.extract",         "Building footprints"),
    "population":    ("etl.pipelines.population.extract",        "Population (WorldPop/GHS-POP/LandScan)"),
    "gridded-manifest": ("etl.pipelines.gridded.extract_manifest", "Validate gridded NetCDF inputs + manifest"),
    "gridded-qa":       ("etl.pipelines.gridded.validate_inputs",  "Deep QA on gridded source NetCDF data"),
    "process-flood": ("etl.pipelines.jrc.process_flood",        "JRC flood COGs (mosaic+clip)"),
    "gridded-zarr":  ("etl.pipelines.gridded.process_zarr",     "Stack gridded NetCDF inputs to GeoZarr"),
    "gridded-cogs":  ("etl.pipelines.gridded.process_cogs",     "Export gridded COG derivatives"),
    "gridded-validate": ("etl.pipelines.gridded.validate_output", "Validate gridded Zarr + COG outputs"),
    # IBF
    "ibf-impact":         ("etl.pipelines.ibf.impact_static",    "IBF static impact pre-computation"),
    "ibf-glofas-extract": ("etl.pipelines.ibf.extract_glofas",   "GloFAS forecast download (CDS API)"),
    "ibf-glofas-process": ("etl.pipelines.ibf.process_glofas",   "GloFAS discharge → RP mapping"),
    "ibf-trigger":        ("etl.pipelines.ibf.trigger",           "IBF trigger evaluation"),
    "ibf-export":         ("etl.pipelines.ibf.export",            "IBF alert export"),
}

PHASES = [
    ["gadm"],
    ["gha"],
    ["jrc-flood", "jrc-water", "hydrobasins", "hydrorivers",
     "osm-water", "osm-roads", "dem", "buildings", "population", "gridded-manifest"],
    ["gridded-qa"],
    ["process-flood", "gridded-zarr"],
    ["gridded-cogs"],
    ["gridded-validate"],
    # IBF phases
    ["ibf-impact"],
    ["ibf-glofas-extract"],
    ["ibf-glofas-process"],
    ["ibf-trigger", "ibf-export"],
]


def setup_logging(log_file: Path) -> logging.Logger:
    logger = logging.getLogger("etl")
    logger.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", "%Y-%m-%d %H:%M:%S")
    fh = logging.FileHandler(log_file)
    fh.setFormatter(fmt)
    logger.addHandler(fh)
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(fmt)
    logger.addHandler(ch)
    return logger


def run_step(step_name: str, module_name: str, desc: str) -> tuple[str, str, float]:
    from etl.core.metrics import push_step
    t0 = time.time()
    try:
        mod = importlib.import_module(module_name)
        mod.main()
        dur = time.time() - t0
        push_step(step_name, ok=True, duration=dur)
        return (step_name, "OK", dur)
    except Exception as e:
        tb = traceback.format_exc()
        dur = time.time() - t0
        push_step(step_name, ok=False, duration=dur)
        return (step_name, f"FAILED: {e}\n{tb}", dur)


def send_email(to_addr: str, subject: str, body: str, log_file: Path):
    smtp_host = os.getenv("SMTP_HOST", "smtp.gmail.com")
    smtp_port = int(os.getenv("SMTP_PORT", "587"))
    smtp_user = os.getenv("SMTP_USER", "")
    smtp_pass = os.getenv("SMTP_PASS", "")

    if not smtp_user or not smtp_pass:
        print(f"SMTP not configured — log at {log_file}")
        return

    msg = MIMEMultipart()
    msg["From"] = smtp_user
    msg["To"] = to_addr
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "plain"))

    if log_file.exists():
        att = MIMEText(log_file.read_text(), "plain")
        att.add_header("Content-Disposition", "attachment", filename=log_file.name)
        msg.attach(att)

    with smtplib.SMTP(smtp_host, smtp_port) as server:
        server.starttls()
        server.login(smtp_user, smtp_pass)
        server.send_message(msg)
    print(f"Email sent to {to_addr}")


def main():
    parser = argparse.ArgumentParser(description="spatial-db ETL orchestrator")
    parser.add_argument("--only", help="Comma-separated step names")
    parser.add_argument("--skip", help="Comma-separated step names to skip")
    parser.add_argument("--email", help="Email for completion notification")
    parser.add_argument("--parallel", type=int, default=3, help="Max parallel workers")
    parser.add_argument("--list", action="store_true", help="List available steps")
    args = parser.parse_args()

    if args.list:
        print("Available ETL steps:\n")
        for i, phase in enumerate(PHASES, 1):
            print(f"Phase {i}:")
            for name in phase:
                _, desc = STEPS[name]
                print(f"  {name:20s} {desc}")
            print()
        return

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = LOG_DIR / f"etl_{timestamp}.log"
    log = setup_logging(log_file)

    only = set(args.only.split(",")) if args.only else None
    skip = set(args.skip.split(",")) if args.skip else set()

    def include(name):
        if only:
            return name in only
        return name not in skip

    log.info("=" * 60)
    log.info("spatial-db ETL Pipeline")
    log.info(f"Host: {os.uname().nodename}")
    log.info(f"Started: {datetime.now().isoformat()}")
    log.info("=" * 60)

    t_start = time.time()
    results = []

    for phase_num, phase_steps in enumerate(PHASES, 1):
        active = [s for s in phase_steps if s in STEPS and include(s)]
        if not active:
            continue

        log.info(f"\n--- Phase {phase_num}: {', '.join(active)} ---")

        if len(active) == 1:
            name = active[0]
            module, desc = STEPS[name]
            log.info(f"[{name}] {desc}...")
            result = run_step(name, module, desc)
            log.info(f"[{result[0]}] {result[1].split(chr(10))[0]} ({result[2]:.0f}s)")
            results.append(result)
        else:
            with ThreadPoolExecutor(max_workers=args.parallel) as pool:
                futures = {}
                for name in active:
                    module, desc = STEPS[name]
                    log.info(f"[{name}] {desc}... (parallel)")
                    futures[pool.submit(run_step, name, module, desc)] = name

                for fut in as_completed(futures):
                    result = fut.result()
                    log.info(f"[{result[0]}] {result[1].split(chr(10))[0]} ({result[2]:.0f}s)")
                    results.append(result)

    total_time = time.time() - t_start
    ok_count = sum(1 for _, s, _ in results if s == "OK")
    fail_count = len(results) - ok_count

    log.info("\n" + "=" * 60)
    log.info("SUMMARY")
    log.info("=" * 60)
    for name, status, dur in results:
        icon = "OK" if status == "OK" else "FAIL"
        log.info(f"  [{icon}] {name:20s} {dur:8.0f}s")
    log.info(f"\nTotal: {ok_count} ok, {fail_count} failed, {total_time:.0f}s")
    log.info(f"Log: {log_file}")

    # Push summary metrics
    from etl.core.metrics import push_summary
    push_summary(ok_count, fail_count, total_time)

    if args.email:
        subject = f"spatial-db ETL {'DONE' if fail_count == 0 else 'FAILED'} — {ok_count}/{len(results)}"
        body = f"Host: {os.uname().nodename}\nTime: {total_time:.0f}s\n\n"
        for name, status, dur in results:
            body += f"{'OK' if status == 'OK' else 'FAIL':5s} {name:20s} {dur:.0f}s\n"
        body += f"\nFull log attached."
        try:
            send_email(args.email, subject, body, log_file)
        except Exception as e:
            log.error(f"Email failed: {e}")


if __name__ == "__main__":
    main()
