"""Prefect flow and workflow retry helpers for Day 4."""

from __future__ import annotations

import time

from prefect import flow

from day4.pipeline.config import Day4Config
from day4.pipeline.logging_utils import LOGGER
from day4.pipeline.models import WorkflowStepResult
from day4.pipeline.sabotage import maybe_trigger_sabotage
from day4.pipeline.tasks import provision_day3_views, run_day2_pipeline, run_day3_export


@flow(name="day4_orchestrated_workflow", log_prints=True)
def day4_flow(config: Day4Config) -> list[WorkflowStepResult]:
    """Run the full Day 4 orchestration sequence once."""

    LOGGER.info("Starting Day 4 flow", extra={"context": {"sabotage_target": config.sabotage_target}})
    step_results: list[WorkflowStepResult] = []
    step_results.append(run_day2_pipeline(config))
    step_results.append(provision_day3_views(config))
    step_results.append(run_day3_export(config))

    maybe_trigger_sabotage("workflow_checkpoint", config=config)
    return step_results


def run_with_workflow_retries(config: Day4Config) -> list[WorkflowStepResult]:
    """Retry the entire workflow when a non-task failure occurs."""

    last_error: Exception | None = None

    for attempt in range(1, config.workflow_retries + 1):
        try:
            LOGGER.info(
                "Starting workflow attempt",
                extra={"context": {"attempt": attempt, "max_attempts": config.workflow_retries}},
            )
            return day4_flow(config)
        except Exception as exc:
            last_error = exc
            LOGGER.warning(
                "Workflow attempt failed",
                extra={
                    "context": {
                        "attempt": attempt,
                        "max_attempts": config.workflow_retries,
                        "error": str(exc),
                    }
                },
            )
            if attempt >= config.workflow_retries:
                raise
            time.sleep(config.workflow_retry_delay_seconds)

    if last_error is not None:
        raise last_error

    raise RuntimeError("Day 4 workflow exited without producing a result")
