"""Runtime configuration for the Day 4 workflow."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

from day4.pipeline.constants import (
    ALLOWED_SABOTAGE_MODES,
    ALLOWED_SABOTAGE_TARGETS,
    DEFAULT_OUTPUT_DIR,
    DEFAULT_SCHEDULE_EVERY_HOURS,
    DEFAULT_TASK_RETRIES,
    DEFAULT_TASK_RETRY_DELAY_SECONDS,
    DEFAULT_WORKFLOW_RETRIES,
    DEFAULT_WORKFLOW_RETRY_DELAY_SECONDS,
    REPO_ROOT,
)


load_dotenv()
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

        return REPO_ROOT / "day3" / "output" / "day3_clean_papers_benchmark.json"


def parse_positive_int(value: str | None, field_name: str, default: int) -> int:
    """Parse a positive integer with a safe fallback default."""

    if value is None or not str(value).strip():
        return default

    try:
        parsed_value = int(value)
    except ValueError as exc:
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
    except ValueError as exc:
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
