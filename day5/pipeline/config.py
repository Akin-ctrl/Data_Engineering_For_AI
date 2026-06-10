"""Runtime configuration for the Day 5 instruction payload pipeline."""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

from day5.pipeline.constants import (
    DEFAULT_CHUNK_OVERLAP_WORDS,
    DEFAULT_EXPORT_BASENAME,
    DEFAULT_MAX_CHUNK_WORDS,
    DEFAULT_MAX_PAPERS,
    DEFAULT_MIN_INPUT_WORDS,
    DEFAULT_MIN_OUTPUT_WORDS,
    DEFAULT_ORDER_BY,
    DEFAULT_OUTPUT_DIR,
    DEFAULT_SOURCE_TABLE,
    DEFAULT_SPLIT_SEED,
    DEFAULT_TRAIN_RATIO,
    EXPORT_BASENAME_PATTERN,
    IDENTIFIER_PATTERN,
    SIMPLE_IDENTIFIER_PATTERN,
)


@dataclass(frozen=True)
class Day5Config:
    """Typed configuration for Day 5 instruction payload generation."""

    pghost: str
    pgport: int
    pgdatabase: str
    pguser: str
    pgpassword: str
    source_table: str
    order_by: str
    output_dir: Path
    export_basename: str
    max_chunk_words: int
    chunk_overlap_words: int
    min_input_words: int
    min_output_words: int
    train_ratio: float
    split_seed: int
    max_papers: int

    @property
    def sqlalchemy_url(self) -> str:
        """Build SQLAlchemy URL for PostgreSQL access."""

        return (
            f"postgresql+psycopg://{self.pguser}:{self.pgpassword}"
            f"@{self.pghost}:{self.pgport}/{self.pgdatabase}"
        )

    @property
    def alpaca_all_path(self) -> Path:
        """Full Alpaca-style payload path."""

        return self.output_dir / f"{self.export_basename}_alpaca_all.jsonl"

    @property
    def alpaca_train_path(self) -> Path:
        """Train split Alpaca-style payload path."""

        return self.output_dir / f"{self.export_basename}_alpaca_train.jsonl"

    @property
    def alpaca_val_path(self) -> Path:
        """Validation split Alpaca-style payload path."""

        return self.output_dir / f"{self.export_basename}_alpaca_val.jsonl"

    @property
    def chat_all_path(self) -> Path:
        """Full chat-style payload path."""

        return self.output_dir / f"{self.export_basename}_chat_all.jsonl"

    @property
    def chat_train_path(self) -> Path:
        """Train split chat-style payload path."""

        return self.output_dir / f"{self.export_basename}_chat_train.jsonl"

    @property
    def chat_val_path(self) -> Path:
        """Validation split chat-style payload path."""

        return self.output_dir / f"{self.export_basename}_chat_val.jsonl"

    @property
    def manifest_path(self) -> Path:
        """Manifest path with run summary and data quality metrics."""

        return self.output_dir / f"{self.export_basename}_manifest.json"


def validate_identifier(value: str, *, pattern: re.Pattern[str], field_name: str) -> str:
    """Validate SQL identifier-like values before SQL interpolation."""

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


def parse_ratio(value: str | None, field_name: str, default: float) -> float:
    """Parse a split ratio in the open interval (0, 1)."""

    if value is None or not str(value).strip():
        return default

    try:
        parsed_value = float(value)
    except ValueError as exc:
        raise ValueError(f"{field_name} must be a float, got: {value}") from exc

    if parsed_value <= 0 or parsed_value >= 1:
        raise ValueError(f"{field_name} must be between 0 and 1, got: {parsed_value}")

    return parsed_value


def resolve_relative_path(path_value: str, *, base_dir: Path) -> Path:
    """Resolve environment path values against repository root."""

    path = Path(path_value).expanduser()
    if path.is_absolute():
        return path
    return base_dir / path


def load_config() -> Day5Config:
    """Load and validate Day 5 configuration from environment variables."""

    load_dotenv()

    required = ["PGHOST", "PGPORT", "PGDATABASE", "PGUSER", "PGPASSWORD"]
    missing = [name for name in required if not os.getenv(name)]
    if missing:
        raise ValueError(f"Missing required environment variables: {', '.join(missing)}")

    repo_root = Path(__file__).resolve().parents[2]
    source_table = validate_identifier(
        os.getenv("DAY5_SOURCE_TABLE", DEFAULT_SOURCE_TABLE),
        pattern=IDENTIFIER_PATTERN,
        field_name="DAY5_SOURCE_TABLE",
    )
    order_by = validate_identifier(
        os.getenv("DAY5_ORDER_BY", DEFAULT_ORDER_BY),
        pattern=SIMPLE_IDENTIFIER_PATTERN,
        field_name="DAY5_ORDER_BY",
    )
    output_dir = resolve_relative_path(
        os.getenv("DAY5_OUTPUT_DIR", DEFAULT_OUTPUT_DIR),
        base_dir=repo_root,
    )
    export_basename = validate_identifier(
        os.getenv("DAY5_EXPORT_BASENAME", DEFAULT_EXPORT_BASENAME),
        pattern=EXPORT_BASENAME_PATTERN,
        field_name="DAY5_EXPORT_BASENAME",
    )

    max_chunk_words = parse_positive_int(
        os.getenv("DAY5_MAX_CHUNK_WORDS"),
        "DAY5_MAX_CHUNK_WORDS",
        DEFAULT_MAX_CHUNK_WORDS,
    )
    chunk_overlap_words = parse_non_negative_int(
        os.getenv("DAY5_CHUNK_OVERLAP_WORDS"),
        "DAY5_CHUNK_OVERLAP_WORDS",
        DEFAULT_CHUNK_OVERLAP_WORDS,
    )
    if chunk_overlap_words >= max_chunk_words:
        raise ValueError("DAY5_CHUNK_OVERLAP_WORDS must be smaller than DAY5_MAX_CHUNK_WORDS")

    min_input_words = parse_positive_int(
        os.getenv("DAY5_MIN_INPUT_WORDS"),
        "DAY5_MIN_INPUT_WORDS",
        DEFAULT_MIN_INPUT_WORDS,
    )
    min_output_words = parse_positive_int(
        os.getenv("DAY5_MIN_OUTPUT_WORDS"),
        "DAY5_MIN_OUTPUT_WORDS",
        DEFAULT_MIN_OUTPUT_WORDS,
    )
    train_ratio = parse_ratio(
        os.getenv("DAY5_TRAIN_RATIO"),
        "DAY5_TRAIN_RATIO",
        DEFAULT_TRAIN_RATIO,
    )
    split_seed = parse_positive_int(
        os.getenv("DAY5_SPLIT_SEED"),
        "DAY5_SPLIT_SEED",
        DEFAULT_SPLIT_SEED,
    )
    max_papers = parse_non_negative_int(
        os.getenv("DAY5_MAX_PAPERS"),
        "DAY5_MAX_PAPERS",
        DEFAULT_MAX_PAPERS,
    )

    return Day5Config(
        pghost=os.environ["PGHOST"],
        pgport=parse_positive_int(os.environ["PGPORT"], "PGPORT", 5432),
        pgdatabase=os.environ["PGDATABASE"],
        pguser=os.environ["PGUSER"],
        pgpassword=os.environ["PGPASSWORD"],
        source_table=source_table,
        order_by=order_by,
        output_dir=output_dir,
        export_basename=export_basename,
        max_chunk_words=max_chunk_words,
        chunk_overlap_words=chunk_overlap_words,
        min_input_words=min_input_words,
        min_output_words=min_output_words,
        train_ratio=train_ratio,
        split_seed=split_seed,
        max_papers=max_papers,
    )
