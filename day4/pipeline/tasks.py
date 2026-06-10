"""Prefect tasks for the Day 4 workflow."""

from __future__ import annotations

import sys
from datetime import datetime, timezone

from prefect import get_run_logger, task

from day4.pipeline.config import Day4Config, TASK_RETRIES, TASK_RETRY_DELAY_SECONDS
from day4.pipeline.constants import DAY2_SCRIPT, DAY3_SCRIPT, DAY3_VIEWS_SQL
from day4.pipeline.models import WorkflowStepResult
from day4.pipeline.sabotage import maybe_trigger_sabotage
from day4.pipeline.subprocess_utils import run_subprocess


@task(name="Run Day 2 pipeline", retries=TASK_RETRIES, retry_delay_seconds=TASK_RETRY_DELAY_SECONDS)
def run_day2_pipeline(config: Day4Config) -> WorkflowStepResult:
    """Execute the Day 2 ingestion script."""

    logger = get_run_logger()
    started_at = datetime.now(timezone.utc).isoformat()
    maybe_trigger_sabotage("day2_pipeline", config=config)
    run_subprocess([sys.executable, str(DAY2_SCRIPT)], config=config, step_name="day2_pipeline")
    finished_at = datetime.now(timezone.utc).isoformat()
    logger.info("Day 2 pipeline finished")
    return WorkflowStepResult(
        name="day2_pipeline",
        status="completed",
        command=f"{sys.executable} {DAY2_SCRIPT}",
        started_at=started_at,
        finished_at=finished_at,
    )


@task(name="Provision Day 3 views", retries=TASK_RETRIES, retry_delay_seconds=TASK_RETRY_DELAY_SECONDS)
def provision_day3_views(config: Day4Config) -> WorkflowStepResult:
    """Create the reusable Day 3 query views and materialized views."""

    logger = get_run_logger()
    started_at = datetime.now(timezone.utc).isoformat()
    maybe_trigger_sabotage("day3_views", config=config)
    command = [
        "psql",
        "-v",
        "ON_ERROR_STOP=1",
        "-P",
        "pager=off",
        "-h",
        config.pghost,
        "-p",
        str(config.pgport),
        "-U",
        config.pguser,
        "-d",
        config.pgdatabase,
        "-f",
        str(DAY3_VIEWS_SQL),
    ]
    run_subprocess(command, config=config, step_name="day3_views")
    finished_at = datetime.now(timezone.utc).isoformat()
    logger.info("Day 3 views provisioned")
    return WorkflowStepResult(
        name="day3_views",
        status="completed",
        command=" ".join(command),
        started_at=started_at,
        finished_at=finished_at,
    )


@task(name="Run Day 3 export", retries=TASK_RETRIES, retry_delay_seconds=TASK_RETRY_DELAY_SECONDS)
def run_day3_export(config: Day4Config) -> WorkflowStepResult:
    """Execute the Day 3 export and benchmark script."""

    logger = get_run_logger()
    started_at = datetime.now(timezone.utc).isoformat()
    maybe_trigger_sabotage("day3_export", config=config)
    run_subprocess([sys.executable, str(DAY3_SCRIPT)], config=config, step_name="day3_export")
    finished_at = datetime.now(timezone.utc).isoformat()
    logger.info("Day 3 export finished")
    return WorkflowStepResult(
        name="day3_export",
        status="completed",
        command=f"{sys.executable} {DAY3_SCRIPT}",
        started_at=started_at,
        finished_at=finished_at,
    )
