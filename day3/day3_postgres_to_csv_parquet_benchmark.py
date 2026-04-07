"""Day 3 pipeline: export clean PostgreSQL data to CSV and Parquet, then benchmark reads.

This lab shows why columnar storage matters by taking the clean Day 2 data from
PostgreSQL, writing the same dataset to CSV and Parquet, and timing repeated reads
with both pandas and PyArrow.
"""

from __future__ import annotations

import gc
import json
import logging
import os
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean, median
from typing import Any, Callable

import pandas as pd
import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as pq
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine


SCHEMA_NAME = "training_data"
DEFAULT_SOURCE_TABLE = "training_data.clean_papers"
DEFAULT_ORDER_BY = "paper_key"
DEFAULT_OUTPUT_DIR = "day3/output"
DEFAULT_BENCHMARK_RUNS = 10
DEFAULT_EXPORT_BASENAME = "day3_clean_papers_benchmark"
IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)*$")
SIMPLE_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


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
    """Create the Day 3 logger once with JSON output."""

    logger = logging.getLogger("day3_benchmark")
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(JsonFormatter())
        logger.addHandler(handler)

    return logger


LOGGER = configure_logger()


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

        return self.output_dir / f"{self.export_basename}_benchmark.json"


@dataclass(frozen=True)
class ReadBenchmarkResult:
    """Timing results for one reader and one file format."""

    reader: str
    file_format: str
    rows_read: int
    run_count: int
    average_ms: float
    median_ms: float
    min_ms: float
    max_ms: float


@dataclass(frozen=True)
class FileArtifact:
    """Metadata for one exported file."""

    file_format: str
    path: Path
    size_bytes: int

    @property
    def size_mb(self) -> float:
        """Return the artifact size in megabytes."""

        return self.size_bytes / 1024 / 1024


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
    except ValueError as exc:  # pragma: no cover - defensive branch
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

    repo_root = Path(__file__).resolve().parents[1]
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
        pattern=re.compile(r"^[A-Za-z0-9_-]+$"),
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


class Day3BenchmarkPipeline:
    """Export Day 2 data to CSV/Parquet and benchmark read performance."""

    def __init__(self, config: Day3Config) -> None:
        self.config = config
        self.engine = create_engine(self.config.sqlalchemy_url, future=True)

    def run(self) -> None:
        """Execute the full Day 3 export and benchmark workflow."""

        self.config.output_dir.mkdir(parents=True, exist_ok=True)
        source_frame = self.load_source_frame()
        prepared_frame = self.prepare_export_frame(source_frame)
        csv_artifact, parquet_artifact = self.write_exports(prepared_frame)
        results = self.run_benchmarks()
        report = self.build_report(prepared_frame, csv_artifact, parquet_artifact, results)
        self.write_report(report)
        self.print_summary_table(csv_artifact, parquet_artifact, results)

    def load_source_frame(self) -> pd.DataFrame:
        """Load the clean Day 2 table from PostgreSQL into a pandas DataFrame."""

        source_sql = self.build_source_sql()
        LOGGER.info(
            "Loading source data from PostgreSQL",
            extra={
                "context": {
                    "source_table": self.config.source_table,
                    "order_by": self.config.order_by,
                }
            },
        )

        with self.engine.begin() as connection:
            frame = pd.read_sql_query(text(source_sql), connection)

        if frame.empty:
            raise ValueError("The Day 3 source query returned no rows")

        LOGGER.info(
            "Source data loaded",
            extra={
                "context": {
                    "rows": int(len(frame)),
                    "columns": list(frame.columns),
                }
            },
        )
        return frame

    def build_source_sql(self) -> str:
        """Build a safe SQL query that selects the analytical Day 2 columns."""

        return f"""
        SELECT
            paper_key,
            versioned_id,
            batch_id,
            title,
            summary,
            published_at,
            updated_at,
            primary_category,
            categories_json::text AS categories_json,
            author_count,
            first_author,
            authors_preview,
            arxiv_url,
            pdf_url,
            comment,
            journal_ref,
            doi,
            raw_payload::text AS raw_payload,
            loaded_at
        FROM {self.config.source_table}
        ORDER BY {self.config.order_by}
        """.strip()

    def prepare_export_frame(self, frame: pd.DataFrame) -> pd.DataFrame:
        """Create a stable export frame suitable for both CSV and Parquet."""

        return frame.copy()

    def write_exports(self, frame: pd.DataFrame) -> tuple[FileArtifact, FileArtifact]:
        """Write the export DataFrame to CSV and Parquet, overwriting deterministically."""

        csv_path = self.config.csv_path
        parquet_path = self.config.parquet_path

        frame.to_csv(csv_path, index=False, encoding="utf-8")
        table = pa.Table.from_pandas(frame, preserve_index=False)
        pq.write_table(table, parquet_path, compression="snappy")

        csv_artifact = FileArtifact(
            file_format="csv",
            path=csv_path,
            size_bytes=csv_path.stat().st_size,
        )
        parquet_artifact = FileArtifact(
            file_format="parquet",
            path=parquet_path,
            size_bytes=parquet_path.stat().st_size,
        )

        LOGGER.info(
            "Export files written",
            extra={
                "context": {
                    "csv_path": str(csv_artifact.path),
                    "csv_size_mb": round(csv_artifact.size_mb, 3),
                    "parquet_path": str(parquet_artifact.path),
                    "parquet_size_mb": round(parquet_artifact.size_mb, 3),
                }
            },
        )
        return csv_artifact, parquet_artifact

    def run_benchmarks(self) -> list[ReadBenchmarkResult]:
        """Run repeated read benchmarks against both file formats."""

        readers: list[tuple[str, str, Callable[[], Any], Callable[[Any], int]]] = [
            (
                "pandas",
                "csv",
                lambda: pd.read_csv(self.config.csv_path),
                lambda payload: int(len(payload)),
            ),
            (
                "pandas",
                "parquet",
                lambda: pd.read_parquet(self.config.parquet_path, engine="pyarrow"),
                lambda payload: int(len(payload)),
            ),
            (
                "pyarrow",
                "csv",
                lambda: pacsv.read_csv(self.config.csv_path),
                lambda payload: int(payload.num_rows),
            ),
            (
                "pyarrow",
                "parquet",
                lambda: pq.read_table(self.config.parquet_path),
                lambda payload: int(payload.num_rows),
            ),
        ]

        for _, _, read_func, row_counter in readers:
            warmup_payload = read_func()
            row_counter(warmup_payload)
        gc.collect()

        results: list[ReadBenchmarkResult] = []
        for reader_name, file_format, read_func, row_counter in readers:
            result = self.measure_reader(
                reader_name=reader_name,
                file_format=file_format,
                read_func=read_func,
                row_counter=row_counter,
            )
            results.append(result)
            LOGGER.info(
                "Benchmark completed",
                extra={
                    "context": {
                        "reader": reader_name,
                        "file_format": file_format,
                        "average_ms": round(result.average_ms, 3),
                        "median_ms": round(result.median_ms, 3),
                        "rows_read": result.rows_read,
                    }
                },
            )

        return results

    def measure_reader(
        self,
        *,
        reader_name: str,
        file_format: str,
        read_func: Callable[[], Any],
        row_counter: Callable[[Any], int],
    ) -> ReadBenchmarkResult:
        """Time repeated reads for a single reader and file format."""

        durations_ms: list[float] = []
        rows_read = 0

        for _ in range(self.config.benchmark_runs):
            gc.collect()
            started_ns = time.perf_counter_ns()
            payload = read_func()
            elapsed_ms = (time.perf_counter_ns() - started_ns) / 1_000_000
            durations_ms.append(elapsed_ms)
            rows_read = row_counter(payload)

        return ReadBenchmarkResult(
            reader=reader_name,
            file_format=file_format,
            rows_read=rows_read,
            run_count=self.config.benchmark_runs,
            average_ms=round(mean(durations_ms), 3),
            median_ms=round(median(durations_ms), 3),
            min_ms=round(min(durations_ms), 3),
            max_ms=round(max(durations_ms), 3),
        )

    def build_report(
        self,
        frame: pd.DataFrame,
        csv_artifact: FileArtifact,
        parquet_artifact: FileArtifact,
        results: list[ReadBenchmarkResult],
    ) -> dict[str, Any]:
        """Build the structured JSON report for the Day 3 benchmark."""

        summary_rows = self.build_summary_rows(csv_artifact, parquet_artifact, results)
        benchmark_rows = [result.__dict__ for result in results]

        return {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "source": {
                "table": self.config.source_table,
                "order_by": self.config.order_by,
                "rows": int(len(frame)),
                "columns": list(frame.columns),
            },
            "exports": {
                "csv": {
                    "path": str(csv_artifact.path),
                    "size_bytes": csv_artifact.size_bytes,
                    "size_mb": round(csv_artifact.size_mb, 3),
                },
                "parquet": {
                    "path": str(parquet_artifact.path),
                    "size_bytes": parquet_artifact.size_bytes,
                    "size_mb": round(parquet_artifact.size_mb, 3),
                },
            },
            "benchmarks": benchmark_rows,
            "comparison": summary_rows,
        }

    def build_summary_rows(
        self,
        csv_artifact: FileArtifact,
        parquet_artifact: FileArtifact,
        results: list[ReadBenchmarkResult],
    ) -> list[dict[str, Any]]:
        """Create the classroom-friendly comparison rows for pandas and PyArrow."""

        grouped: dict[str, dict[str, ReadBenchmarkResult]] = {"pandas": {}, "pyarrow": {}}
        for result in results:
            grouped[result.reader][result.file_format] = result

        summary_rows: list[dict[str, Any]] = []
        for reader_name in ["pandas", "pyarrow"]:
            csv_result = grouped[reader_name]["csv"]
            parquet_result = grouped[reader_name]["parquet"]
            ms_delta = csv_result.average_ms - parquet_result.average_ms
            speedup_factor = csv_result.average_ms / parquet_result.average_ms
            size_delta_mb = csv_artifact.size_mb - parquet_artifact.size_mb
            size_reduction_pct = (size_delta_mb / csv_artifact.size_mb) * 100 if csv_artifact.size_mb else 0.0

            summary_rows.append(
                {
                    "reader": reader_name,
                    "csv_avg_ms": csv_result.average_ms,
                    "parquet_avg_ms": parquet_result.average_ms,
                    "ms_delta": round(ms_delta, 3),
                    "speedup_factor": round(speedup_factor, 3),
                    "csv_size_mb": round(csv_artifact.size_mb, 3),
                    "parquet_size_mb": round(parquet_artifact.size_mb, 3),
                    "mb_delta": round(size_delta_mb, 3),
                    "size_reduction_pct": round(size_reduction_pct, 3),
                }
            )

        return summary_rows

    def write_report(self, report: dict[str, Any]) -> None:
        """Persist the benchmark report as JSON."""

        self.config.report_path.write_text(
            json.dumps(report, indent=2, ensure_ascii=True, default=str),
            encoding="utf-8",
        )
        LOGGER.info(
            "Benchmark report written",
            extra={
                "context": {
                    "report_path": str(self.config.report_path),
                }
            },
        )

    def print_summary_table(
        self,
        csv_artifact: FileArtifact,
        parquet_artifact: FileArtifact,
        results: list[ReadBenchmarkResult],
    ) -> None:
        """Print a concise comparison table for the classroom."""

        summary_rows = self.build_summary_rows(csv_artifact, parquet_artifact, results)
        summary_frame = pd.DataFrame(summary_rows)
        print("\nDay 3 Benchmark Summary")
        print(summary_frame.to_string(index=False))
        print()


def main() -> None:
    """Entry point for the Day 3 benchmark pipeline."""

    config = load_config()
    pipeline = Day3BenchmarkPipeline(config)
    pipeline.run()


if __name__ == "__main__":
    main()
