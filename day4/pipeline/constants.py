"""Shared constants and paths for the Day 4 workflow."""

from __future__ import annotations

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_OUTPUT_DIR = "day4/output"
DEFAULT_WORKFLOW_RETRIES = 2
DEFAULT_WORKFLOW_RETRY_DELAY_SECONDS = 10
DEFAULT_TASK_RETRIES = 2
DEFAULT_TASK_RETRY_DELAY_SECONDS = 5
DEFAULT_SCHEDULE_EVERY_HOURS = 24
ALLOWED_SABOTAGE_TARGETS = {
    "off",
    "day2_pipeline",
    "day3_views",
    "day3_export",
    "workflow_checkpoint",
}
ALLOWED_SABOTAGE_MODES = {"off", "once", "always"}
DAY2_SCRIPT = REPO_ROOT / "day2" / "day2_arxiv_api_to_postgres.py"
DAY3_VIEWS_SQL = REPO_ROOT / "day3" / "day3_agent_query_views.sql"
DAY3_SCRIPT = REPO_ROOT / "day3" / "day3_postgres_to_csv_parquet_benchmark.py"
