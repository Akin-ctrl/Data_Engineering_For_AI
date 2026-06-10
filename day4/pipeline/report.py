"""Report helpers for the Day 4 workflow."""

from __future__ import annotations

import json
from dataclasses import asdict
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import create_engine, text

from day4.pipeline.config import Day4Config, TASK_RETRIES, TASK_RETRY_DELAY_SECONDS
from day4.pipeline.logging_utils import LOGGER
from day4.pipeline.models import WorkflowReport, WorkflowStepResult


def collect_database_snapshot(config: Day4Config) -> dict[str, Any]:
    """Collect a lightweight database snapshot for the Day 4 report."""

    engine = create_engine(config.sqlalchemy_url, future=True)
    queries = {
        "clean_papers": "SELECT count(*) FROM training_data.clean_papers",
        "paper_authors": "SELECT count(*) FROM training_data.paper_authors",
        "pipeline_state": "SELECT count(*) FROM training_data.pipeline_state",
        "v_agent_papers": "SELECT count(*) FROM training_data.v_agent_papers",
        "mv_agent_keyword_frequency": "SELECT count(*) FROM training_data.mv_agent_keyword_frequency",
    }

    snapshot: dict[str, Any] = {}
    with engine.begin() as connection:
        for name, query in queries.items():
            snapshot[name] = int(connection.execute(text(query)).scalar_one())

    return snapshot


def load_day3_report_summary(config: Day4Config) -> dict[str, Any] | None:
    """Load the Day 3 benchmark report if it exists."""

    if not config.day3_report_path.exists():
        return None

    report = json.loads(config.day3_report_path.read_text(encoding="utf-8"))
    return {
        "source_rows": report.get("source", {}).get("rows"),
        "csv_size_mb": report.get("exports", {}).get("csv", {}).get("size_mb"),
        "parquet_size_mb": report.get("exports", {}).get("parquet", {}).get("size_mb"),
        "comparison": report.get("comparison", []),
    }


def write_day4_report(config: Day4Config, step_results: list[WorkflowStepResult]) -> None:
    """Persist the orchestration report as JSON."""

    config.output_dir.mkdir(parents=True, exist_ok=True)
    report = WorkflowReport(
        generated_at=datetime.now(timezone.utc).isoformat(),
        sabotage_target=config.sabotage_target,
        sabotage_mode=config.sabotage_mode,
        workflow_retries=config.workflow_retries,
        workflow_retry_delay_seconds=config.workflow_retry_delay_seconds,
        task_retries=TASK_RETRIES,
        task_retry_delay_seconds=TASK_RETRY_DELAY_SECONDS,
        steps=step_results,
        day3_report_path=str(config.day3_report_path),
        day3_report_summary=load_day3_report_summary(config),
        database_snapshot=collect_database_snapshot(config),
    )
    config.day4_report_path.write_text(
        json.dumps(asdict(report), indent=2, ensure_ascii=True, default=str),
        encoding="utf-8",
    )
    LOGGER.info(
        "Day 4 report written",
        extra={"context": {"report_path": str(config.day4_report_path)}},
    )
