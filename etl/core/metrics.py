"""ETL metrics — pushes to Prometheus Pushgateway after each step."""

import os
import time
from prometheus_client import CollectorRegistry, Gauge, push_to_gateway

PUSHGATEWAY = os.getenv("PUSHGATEWAY_URL", "localhost:9091")
JOB_NAME = "spatial_db_etl"

registry = CollectorRegistry()

step_duration = Gauge(
    "etl_step_duration_seconds", "Duration of ETL step",
    ["step"], registry=registry,
)
step_status = Gauge(
    "etl_step_status", "ETL step status (1=ok, 0=fail)",
    ["step"], registry=registry,
)
etl_total_duration = Gauge(
    "etl_total_duration_seconds", "Total ETL duration",
    registry=registry,
)
etl_steps_ok = Gauge(
    "etl_steps_ok", "Number of successful steps",
    registry=registry,
)
etl_steps_failed = Gauge(
    "etl_steps_failed", "Number of failed steps",
    registry=registry,
)
etl_last_run = Gauge(
    "etl_last_run_timestamp", "Timestamp of last ETL run",
    registry=registry,
)


def push_step(step_name: str, ok: bool, duration: float):
    """Push metrics for a completed step."""
    step_duration.labels(step=step_name).set(duration)
    step_status.labels(step=step_name).set(1 if ok else 0)
    try:
        push_to_gateway(PUSHGATEWAY, job=JOB_NAME, registry=registry)
    except Exception:
        pass  # don't fail ETL if pushgateway is down


def push_summary(ok_count: int, fail_count: int, total_duration: float):
    """Push summary metrics at ETL completion."""
    etl_total_duration.set(total_duration)
    etl_steps_ok.set(ok_count)
    etl_steps_failed.set(fail_count)
    etl_last_run.set(time.time())
    try:
        push_to_gateway(PUSHGATEWAY, job=JOB_NAME, registry=registry)
    except Exception:
        pass
