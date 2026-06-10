"""Scheduled serving helpers for the Day 4 workflow."""

from __future__ import annotations

from prefect import flow

from day4.pipeline.config import Day4Config, load_config
from day4.pipeline.logging_utils import LOGGER
from day4.pipeline.paths import ensure_paths_exist
from day4.pipeline.report import write_day4_report
from day4.pipeline.workflow import run_with_workflow_retries


def resolve_schedule_seconds(config: Day4Config) -> int:
    """Resolve schedule interval in seconds from days/hours settings."""

    if config.schedule_every_days > 0:
        return config.schedule_every_days * 24 * 60 * 60
    return config.schedule_every_hours * 60 * 60


@flow(name="day4_orchestrated_workflow_scheduled", log_prints=True)
def day4_scheduled_flow() -> None:
    """Scheduled wrapper flow used by Prefect native schedule serving."""

    config = load_config()
    ensure_paths_exist()
    config.output_dir.mkdir(parents=True, exist_ok=True)
    step_results = run_with_workflow_retries(config)
    write_day4_report(config, step_results)


def serve_prefect_schedule(config: Day4Config) -> None:
    """Serve the Day 4 flow with a configurable Prefect native interval schedule."""

    interval_seconds = resolve_schedule_seconds(config)
    LOGGER.info(
        "Starting Prefect native schedule",
        extra={
            "context": {
                "schedule_name": config.schedule_name,
                "interval_seconds": interval_seconds,
                "interval_hours": round(interval_seconds / 3600, 2),
            }
        },
    )

    day4_scheduled_flow.serve(
        name=config.schedule_name,
        interval=interval_seconds,
    )
