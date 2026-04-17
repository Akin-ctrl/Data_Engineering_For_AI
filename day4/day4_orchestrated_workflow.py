"""Day 4 pipeline: orchestrate the Day 2 and Day 3 labs with Prefect and retries.

This lab shows how to wrap the earlier Day 2 and Day 3 scripts into one workflow.
The workflow runs the API extract first, provisions the query views second, and
runs the Day 3 export/benchmark last so the Parquet file is generated at the end.

The workflow also includes two retry layers:
- task-level retries for temporary step failures
- an outer workflow retry loop for a larger failure boundary
"""

from __future__ import annotations

import json
import logging
import os
import subprocess
import sys
import time
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from prefect import flow, get_run_logger, task
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine


load_dotenv()

REPO_ROOT = Path(__file__).resolve().parents[1]
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


class JsonFormatter(logging.Formatter):
    """Format logs as machine-readable JSON."""

    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
        }

        if hasattr(record, "context"):
            payload["context"] = getattr(record, "context")

        return json.dumps(payload, ensure_ascii=True, default=str)


def configure_logger() -> logging.Logger:
    """Create the Day 4 logger once with JSON output."""

    logger = logging.getLogger("day4_orchestrator")
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(JsonFormatter())
        logger.addHandler(handler)

    return logger


LOGGER = configure_logger()
TASK_RETRIES = int(os.getenv("DAY4_TASK_RETRIES", str(DEFAULT_TASK_RETRIES)))
TASK_RETRY_DELAY_SECONDS = int(os.getenv("DAY4_TASK_RETRY_DELAY_SECONDS", str(DEFAULT_TASK_RETRY_DELAY_SECONDS)))


@dataclass(frozen=True)
class Day4Config:
    """Typed configuration for the orchestrated Day 4 workflow."""

    pghost: str
    pgport: int
    pgdatabase: str
    pguser: str
    pgpassword: str
    output_dir: Path
    workflow_retries: int
    workflow_retry_delay_seconds: int
    sabotage_target: str
    sabotage_mode: str
    schedule_enabled: bool
    schedule_every_hours: int
    schedule_every_days: int
    schedule_name: str

    @property
    def sqlalchemy_url(self) -> str:
        """Build the SQLAlchemy URL for PostgreSQL access."""

        return (
            f"postgresql+psycopg://{self.pguser}:{self.pgpassword}"
            f"@{self.pghost}:{self.pgport}/{self.pgdatabase}"
        )

    @property
    def subprocess_env(self) -> dict[str, str]:
        """Return the environment used by the child lab scripts."""

        env = os.environ.copy()
        env["PYTHONUNBUFFERED"] = "1"
        return env

    @property
    def sabotage_marker_path(self) -> Path:
        """Path used to make a one-time sabotage fail only once."""

        return self.output_dir / f".day4_sabotage_{self.sabotage_target}.flag"

    @property
    def day4_report_path(self) -> Path:
        """Path for the orchestration report."""

        return self.output_dir / "day4_orchestration_report.json"

    @property
    def day3_report_path(self) -> Path:
        """Path for the Day 3 benchmark report."""

        return REPO_ROOT / "day3" / "output" / "day3_clean_papers_benchmark_benchmark.json"


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


def parse_positive_int(value: str | None, field_name: str, default: int) -> int:
    """Parse a positive integer with a safe fallback default."""

    if value is None or not str(value).strip():
        return default

    try:
        parsed_value = int(value)
    except ValueError as exc:  # pragma: no cover - defensive branch
        raise ValueError(f"{field_name} must be an integer, got: {value}") from exc

    if parsed_value <= 0:
        raise ValueError(f"{field_name} must be greater than zero, got: {parsed_value}")

    return parsed_value


def parse_non_negative_int(value: str | None, field_name: str, default: int) -> int:
    """Parse a non-negative integer with a safe fallback default."""

    if value is None or not str(value).strip():
        return default

    try:
        parsed_value = int(value)
    except ValueError as exc:  # pragma: no cover - defensive branch
        raise ValueError(f"{field_name} must be an integer, got: {value}") from exc

    if parsed_value < 0:
        raise ValueError(f"{field_name} must be zero or greater, got: {parsed_value}")

    return parsed_value


def parse_bool(value: str | None, default: bool) -> bool:
    """Parse a boolean-ish environment value with a safe default."""

    if value is None or not value.strip():
        return default

    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "y", "on"}:
        return True
    if normalized in {"0", "false", "no", "n", "off"}:
        return False

    raise ValueError(f"Invalid boolean value: {value}")


def resolve_relative_path(path_value: str, *, base_dir: Path) -> Path:
    """Resolve a path from the environment against the repository root."""

    path = Path(path_value).expanduser()
    if path.is_absolute():
        return path
    return base_dir / path


def validate_choice(value: str, *, field_name: str, allowed_values: set[str]) -> str:
    """Validate a string value against an allowed set."""

    cleaned_value = value.strip()
    if not cleaned_value:
        raise ValueError(f"{field_name} must not be empty")
    if cleaned_value not in allowed_values:
        raise ValueError(f"{field_name} must be one of: {', '.join(sorted(allowed_values))}")
    return cleaned_value


def load_config() -> Day4Config:
    """Load and validate Day 4 configuration from environment variables."""

    required = ["PGHOST", "PGPORT", "PGDATABASE", "PGUSER", "PGPASSWORD"]
    missing = [name for name in required if not os.getenv(name)]
    if missing:
        raise ValueError(f"Missing required environment variables: {', '.join(missing)}")

    output_dir = resolve_relative_path(
        os.getenv("DAY4_OUTPUT_DIR", DEFAULT_OUTPUT_DIR),
        base_dir=REPO_ROOT,
    )
    workflow_retries = parse_positive_int(
        os.getenv("DAY4_WORKFLOW_RETRIES"),
        "DAY4_WORKFLOW_RETRIES",
        DEFAULT_WORKFLOW_RETRIES,
    )
    workflow_retry_delay_seconds = parse_positive_int(
        os.getenv("DAY4_WORKFLOW_RETRY_DELAY_SECONDS"),
        "DAY4_WORKFLOW_RETRY_DELAY_SECONDS",
        DEFAULT_WORKFLOW_RETRY_DELAY_SECONDS,
    )
    sabotage_target = validate_choice(
        os.getenv("DAY4_SABOTAGE_TARGET", "off"),
        field_name="DAY4_SABOTAGE_TARGET",
        allowed_values=ALLOWED_SABOTAGE_TARGETS,
    )
    sabotage_mode = validate_choice(
        os.getenv("DAY4_SABOTAGE_MODE", "once"),
        field_name="DAY4_SABOTAGE_MODE",
        allowed_values=ALLOWED_SABOTAGE_MODES,
    )
    schedule_enabled = parse_bool(os.getenv("DAY4_SCHEDULE_ENABLED", "false"), default=False)
    schedule_every_hours = parse_positive_int(
        os.getenv("DAY4_SCHEDULE_EVERY_HOURS"),
        "DAY4_SCHEDULE_EVERY_HOURS",
        DEFAULT_SCHEDULE_EVERY_HOURS,
    )
    schedule_every_days = parse_non_negative_int(
        os.getenv("DAY4_SCHEDULE_EVERY_DAYS"),
        "DAY4_SCHEDULE_EVERY_DAYS",
        0,
    )
    schedule_name = os.getenv("DAY4_SCHEDULE_NAME", "day4-orchestrated-every-24h").strip()
    if not schedule_name:
        raise ValueError("DAY4_SCHEDULE_NAME must not be empty")

    return Day4Config(
        pghost=os.environ["PGHOST"],
        pgport=parse_positive_int(os.environ["PGPORT"], "PGPORT", 5432),
        pgdatabase=os.environ["PGDATABASE"],
        pguser=os.environ["PGUSER"],
        pgpassword=os.environ["PGPASSWORD"],
        output_dir=output_dir,
        workflow_retries=workflow_retries,
        workflow_retry_delay_seconds=workflow_retry_delay_seconds,
        sabotage_target=sabotage_target,
        sabotage_mode=sabotage_mode,
        schedule_enabled=schedule_enabled,
        schedule_every_hours=schedule_every_hours,
        schedule_every_days=schedule_every_days,
        schedule_name=schedule_name,
    )


def resolve_schedule_seconds(config: Day4Config) -> int:
    """Resolve schedule interval in seconds from days/hours settings."""

    if config.schedule_every_days > 0:
        return config.schedule_every_days * 24 * 60 * 60
    return config.schedule_every_hours * 60 * 60


def ensure_paths_exist() -> None:
    """Fail fast if the reusable lab scripts are missing."""

    missing_paths = [path for path in [DAY2_SCRIPT, DAY3_VIEWS_SQL, DAY3_SCRIPT] if not path.exists()]
    if missing_paths:
        raise FileNotFoundError(f"Missing required lab files: {', '.join(str(path) for path in missing_paths)}")


def run_subprocess(command: list[str], *, config: Day4Config, step_name: str) -> None:
    """Run a child lab command with the project's environment."""

    LOGGER.info(
        "Running step",
        extra={
            "context": {
                "step": step_name,
                "command": " ".join(command),
            }
        },
    )
    subprocess.run(command, cwd=REPO_ROOT, env=config.subprocess_env, check=True)


def maybe_trigger_sabotage(stage: str, *, config: Day4Config) -> None:
    """Fail a stage on purpose so retry behavior is visible in the lab."""

    if config.sabotage_target == "off" or config.sabotage_target != stage:
        return

    config.output_dir.mkdir(parents=True, exist_ok=True)

    if config.sabotage_mode == "always":
        raise RuntimeError(f"Intentional Day 4 sabotage triggered for stage: {stage}")

    marker_path = config.sabotage_marker_path
    if not marker_path.exists():
        marker_path.write_text(f"triggered_at={datetime.now(timezone.utc).isoformat()}\n", encoding="utf-8")
        raise RuntimeError(f"Intentional Day 4 sabotage triggered for stage: {stage}")


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
    config.day4_report_path.write_text(json.dumps(asdict(report), indent=2, ensure_ascii=True, default=str), encoding="utf-8")
    LOGGER.info(
        "Day 4 report written",
        extra={"context": {"report_path": str(config.day4_report_path)}},
    )


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
        except Exception as exc:  # pragma: no cover - orchestration recovery path
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


def main() -> None:
    """Entry point for the Day 4 orchestrated workflow."""

    ensure_paths_exist()
    config = load_config()

    if config.schedule_enabled:
        serve_prefect_schedule(config)
        return

    config.output_dir.mkdir(parents=True, exist_ok=True)
    step_results = run_with_workflow_retries(config)
    write_day4_report(config, step_results)


if __name__ == "__main__":
    main()
