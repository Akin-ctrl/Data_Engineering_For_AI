"""Read benchmark helpers for Day 3 CSV and Parquet outputs.

Each reader is timed twice against each file:

- A "full" read that pulls back every column.
- A "projected" read that pulls back only the few columns in
  PROJECTION_COLUMNS.

The projected read is the interesting one. CSV is a text file with no index, so
even when you only want three columns the reader still has to walk and parse
every byte of every row to find them. Parquet stores each column separately, so
it reads only the columns you asked for and skips the rest of the file.
"""

from __future__ import annotations

import gc
import time
from dataclasses import dataclass
from statistics import mean, median
from typing import Any, Callable

import pandas as pd

from day3.pipeline.config import Day3Config
from day3.pipeline.constants import (
    PROJECTION_COLUMNS,
    READ_MODE_FULL,
    READ_MODE_PROJECTED,
)
from day3.pipeline.logging_utils import LOGGER
from day3.pipeline.models import ReadBenchmarkResult


@dataclass(frozen=True)
class ReaderCase:
    """One thing to time: a reader, a file format, and how much of it is read."""

    reader: str
    file_format: str
    read_mode: str
    columns_read: int
    read_func: Callable[[], Any]
    row_counter: Callable[[Any], int]


def count_frame_rows(payload: Any) -> int:
    """Count rows in a pandas DataFrame."""

    return int(len(payload))


def count_table_rows(payload: Any) -> int:
    """Count rows in a PyArrow Table."""

    return int(payload.num_rows)


def build_reader_cases(config: Day3Config) -> list[ReaderCase]:
    """Build every reader, file format, and read mode combination to time.

    Args:
        config: Day 3 configuration holding the export file paths.

    Returns:
        The list of cases the benchmark should time, in display order.
    """

    import pyarrow.csv as pacsv
    import pyarrow.parquet as pq

    csv_path = config.csv_path
    parquet_path = config.parquet_path

    # Parquet keeps its schema in the file footer, so this is cheap and saves
    # us reading the whole export just to count the columns.
    full_column_count = len(pq.read_schema(parquet_path).names)
    projected_column_count = len(PROJECTION_COLUMNS)

    projected_csv_options = pacsv.ConvertOptions(include_columns=PROJECTION_COLUMNS)

    return [
        ReaderCase(
            reader="pandas",
            file_format="csv",
            read_mode=READ_MODE_FULL,
            columns_read=full_column_count,
            read_func=lambda: pd.read_csv(csv_path),
            row_counter=count_frame_rows,
        ),
        ReaderCase(
            reader="pandas",
            file_format="parquet",
            read_mode=READ_MODE_FULL,
            columns_read=full_column_count,
            read_func=lambda: pd.read_parquet(parquet_path, engine="pyarrow"),
            row_counter=count_frame_rows,
        ),
        ReaderCase(
            reader="pyarrow",
            file_format="csv",
            read_mode=READ_MODE_FULL,
            columns_read=full_column_count,
            read_func=lambda: pacsv.read_csv(csv_path),
            row_counter=count_table_rows,
        ),
        ReaderCase(
            reader="pyarrow",
            file_format="parquet",
            read_mode=READ_MODE_FULL,
            columns_read=full_column_count,
            read_func=lambda: pq.read_table(parquet_path),
            row_counter=count_table_rows,
        ),
        ReaderCase(
            reader="pandas",
            file_format="csv",
            read_mode=READ_MODE_PROJECTED,
            columns_read=projected_column_count,
            read_func=lambda: pd.read_csv(csv_path, usecols=PROJECTION_COLUMNS),
            row_counter=count_frame_rows,
        ),
        ReaderCase(
            reader="pandas",
            file_format="parquet",
            read_mode=READ_MODE_PROJECTED,
            columns_read=projected_column_count,
            read_func=lambda: pd.read_parquet(
                parquet_path,
                engine="pyarrow",
                columns=PROJECTION_COLUMNS,
            ),
            row_counter=count_frame_rows,
        ),
        ReaderCase(
            reader="pyarrow",
            file_format="csv",
            read_mode=READ_MODE_PROJECTED,
            columns_read=projected_column_count,
            read_func=lambda: pacsv.read_csv(csv_path, convert_options=projected_csv_options),
            row_counter=count_table_rows,
        ),
        ReaderCase(
            reader="pyarrow",
            file_format="parquet",
            read_mode=READ_MODE_PROJECTED,
            columns_read=projected_column_count,
            read_func=lambda: pq.read_table(parquet_path, columns=PROJECTION_COLUMNS),
            row_counter=count_table_rows,
        ),
    ]


def measure_reader(*, benchmark_runs: int, case: ReaderCase) -> ReadBenchmarkResult:
    """Time repeated reads for a single benchmark case.

    Args:
        benchmark_runs: How many times to repeat the read.
        case: The reader, file format, and read mode being timed.

    Returns:
        The timing results for this case.
    """

    durations_ms: list[float] = []
    rows_read = 0

    for _ in range(benchmark_runs):
        gc.collect()
        started_ns = time.perf_counter_ns()
        payload = case.read_func()
        elapsed_ms = (time.perf_counter_ns() - started_ns) / 1_000_000
        durations_ms.append(elapsed_ms)
        rows_read = case.row_counter(payload)

    return ReadBenchmarkResult(
        reader=case.reader,
        file_format=case.file_format,
        read_mode=case.read_mode,
        columns_read=case.columns_read,
        rows_read=rows_read,
        run_count=benchmark_runs,
        average_ms=round(mean(durations_ms), 3),
        median_ms=round(median(durations_ms), 3),
        min_ms=round(min(durations_ms), 3),
        max_ms=round(max(durations_ms), 3),
    )


def run_benchmarks(config: Day3Config) -> list[ReadBenchmarkResult]:
    """Run repeated read benchmarks against both file formats and both read modes."""

    cases = build_reader_cases(config)

    # Read everything once before timing so the operating system file cache is
    # warm. Otherwise the first case measured would look unfairly slow.
    for case in cases:
        warmup_payload = case.read_func()
        case.row_counter(warmup_payload)
    gc.collect()

    results: list[ReadBenchmarkResult] = []
    for case in cases:
        result = measure_reader(benchmark_runs=config.benchmark_runs, case=case)
        results.append(result)
        LOGGER.info(
            "Benchmark completed",
            extra={
                "context": {
                    "reader": result.reader,
                    "file_format": result.file_format,
                    "read_mode": result.read_mode,
                    "columns_read": result.columns_read,
                    "average_ms": result.average_ms,
                    "median_ms": result.median_ms,
                    "rows_read": result.rows_read,
                }
            },
        )

    return results
