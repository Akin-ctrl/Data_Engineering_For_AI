"""Small data models used by the Day 4 workflow."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class WorkflowStepResult:
    """Structured summary for one orchestration step."""

    name: str
    status: str
    command: str
    started_at: str
    finished_at: str
    attempts: int = 1
    detail: str | None = None


@dataclass(frozen=True)
class WorkflowReport:
    """Summary written after the Day 4 workflow finishes."""

    generated_at: str
    sabotage_target: str
    sabotage_mode: str
    workflow_retries: int
    workflow_retry_delay_seconds: int
    task_retries: int
    task_retry_delay_seconds: int
    steps: list[WorkflowStepResult]
    day3_report_path: str
    day3_report_summary: dict[str, Any] | None
    database_snapshot: dict[str, Any]
