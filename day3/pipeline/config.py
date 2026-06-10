"""Runtime configuration for the Day 3 benchmark pipeline."""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

from day3.pipeline.constants import (
    DEFAULT_BENCHMARK_RUNS,
    DEFAULT_EXPORT_BASENAME,
    DEFAULT_ORDER_BY,
    DEFAULT_OUTPUT_DIR,
    DEFAULT_SOURCE_TABLE,
    EXPORT_BASENAME_PATTERN,
    IDENTIFIER_PATTERN,
    SIMPLE_IDENTIFIER_PATTERN,
)


@dataclass(frozen=True)
class Day3Config:
    """Typed configuration for the Day 3 benchmark pipeline."""

    pghost: str
    pgport: int
    pgdatabase: str
    pguser: str
    pgpassword: str
    source_table: str
    order_by: str
    output_dir: Path
    export_basename: str
    benchmark_runs: int

    @property
    def sqlalchemy_url(self) -> str:
        """Build the SQLAlchemy URL for PostgreSQL access."""

        return (
            f"postgresql+psycopg://{self.pguser}:{self.pgpassword}"
            f"@{self.pghost}:{self.pgport}/{self.pgdatabase}"
        )

    @property
    def csv_path(self) -> Path:
        """Path for the deterministic CSV export."""

        return self.output_dir / f"{self.export_basename}.csv"

    @property
    def parquet_path(self) -> Path:
        """Path for the deterministic Parquet export."""

        return self.output_dir / f"{self.export_basename}.parquet"

    @property
    def report_path(self) -> Path:
        """Path for the JSON benchmark report."""

        return self.output_dir / f"{self.export_basename}.json"


def validate_identifier(value: str, *, pattern: re.Pattern[str], field_name: str) -> str:
    """Validate a SQL identifier-like value so it can be interpolated safely."""

    cleaned_value = value.strip()
    if not cleaned_value:
        raise ValueError(f"{field_name} must not be empty")
    if not pattern.match(cleaned_value):
        raise ValueError(f"{field_name} has an invalid format: {cleaned_value}")
    return cleaned_value


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


def resolve_relative_path(path_value: str, *, base_dir: Path) -> Path:
    """Resolve a path from the environment against the repository root."""

    path = Path(path_value).expanduser()
    if path.is_absolute():
        return path
    return base_dir / path


def load_config() -> Day3Config:
    """Load and validate the Day 3 configuration from environment variables."""

    load_dotenv()

    required = ["PGHOST", "PGPORT", "PGDATABASE", "PGUSER", "PGPASSWORD"]
    missing = [name for name in required if not os.getenv(name)]
    if missing:
        raise ValueError(f"Missing required environment variables: {', '.join(missing)}")

    repo_root = Path(__file__).resolve().parents[2]
    source_table = validate_identifier(
        os.getenv("DAY3_SOURCE_TABLE", DEFAULT_SOURCE_TABLE),
        pattern=IDENTIFIER_PATTERN,
        field_name="DAY3_SOURCE_TABLE",
    )
    order_by = validate_identifier(
        os.getenv("DAY3_ORDER_BY", DEFAULT_ORDER_BY),
        pattern=SIMPLE_IDENTIFIER_PATTERN,
        field_name="DAY3_ORDER_BY",
    )
    output_dir = resolve_relative_path(
        os.getenv("DAY3_OUTPUT_DIR", DEFAULT_OUTPUT_DIR),
        base_dir=repo_root,
    )
    export_basename = validate_identifier(
        os.getenv("DAY3_EXPORT_BASENAME", DEFAULT_EXPORT_BASENAME),
        pattern=EXPORT_BASENAME_PATTERN,
        field_name="DAY3_EXPORT_BASENAME",
    )
    benchmark_runs = parse_positive_int(
        os.getenv("DAY3_BENCHMARK_RUNS"),
        "DAY3_BENCHMARK_RUNS",
        DEFAULT_BENCHMARK_RUNS,
    )

    return Day3Config(
        pghost=os.environ["PGHOST"],
        pgport=parse_positive_int(os.environ["PGPORT"], "PGPORT", 5432),
        pgdatabase=os.environ["PGDATABASE"],
        pguser=os.environ["PGUSER"],
        pgpassword=os.environ["PGPASSWORD"],
        source_table=source_table,
        order_by=order_by,
        output_dir=output_dir,
        export_basename=export_basename,
        benchmark_runs=benchmark_runs,
    )
