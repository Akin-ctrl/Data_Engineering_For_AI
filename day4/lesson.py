"""Teachable Day 4 orchestration entrypoint.

Open this file first in class. It shows the orchestration story without starting
inside Prefect task definitions, report builders, or scheduling internals.
"""

from __future__ import annotations

import sys
from pathlib import Path

if __package__ is None or __package__ == "":
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from day4.pipeline.config import load_config
from day4.pipeline.paths import ensure_paths_exist
from day4.pipeline.report import write_day4_report
from day4.pipeline.schedule import serve_prefect_schedule
from day4.pipeline.workflow import run_with_workflow_retries


def run_pipeline() -> None:
    """Run Day 4 as a readable orchestration-and-retry story."""

    ensure_paths_exist()
    config = load_config()

    if config.schedule_enabled:
        serve_prefect_schedule(config)
        return

    config.output_dir.mkdir(parents=True, exist_ok=True)
    step_results = run_with_workflow_retries(config)
    write_day4_report(config, step_results)


def main() -> None:
    """Entry point for the Day 4 orchestrated workflow."""

    run_pipeline()


if __name__ == "__main__":
    main()
