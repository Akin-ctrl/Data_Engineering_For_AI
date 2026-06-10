"""Read benchmark helpers for Day 3 CSV and Parquet outputs."""

from __future__ import annotations

import gc
import time
from statistics import mean, median
from typing import Any, Callable

import pandas as pd

from day3.pipeline.config import Day3Config
from day3.pipeline.logging_utils import LOGGER
from day3.pipeline.models import ReadBenchmarkResult


def measure_reader(
    *,
    benchmark_runs: int,
    reader_name: str,
    file_format: str,
    read_func: Callable[[], Any],
    row_counter: Callable[[Any], int],
) -> ReadBenchmarkResult:
    """Time repeated reads for a single reader and file format."""

    durations_ms: list[float] = []
    rows_read = 0

    for _ in range(benchmark_runs):
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
        run_count=benchmark_runs,
        average_ms=round(mean(durations_ms), 3),
        median_ms=round(median(durations_ms), 3),
        min_ms=round(min(durations_ms), 3),
        max_ms=round(max(durations_ms), 3),
    )


def run_benchmarks(config: Day3Config) -> list[ReadBenchmarkResult]:
    """Run repeated read benchmarks against both file formats."""

    import pyarrow.csv as pacsv
    import pyarrow.parquet as pq

    readers: list[tuple[str, str, Callable[[], Any], Callable[[Any], int]]] = [
        (
            "pandas",
            "csv",
            lambda: pd.read_csv(config.csv_path),
            lambda payload: int(len(payload)),
        ),
        (
            "pandas",
            "parquet",
            lambda: pd.read_parquet(config.parquet_path, engine="pyarrow"),
            lambda payload: int(len(payload)),
        ),
        (
            "pyarrow",
            "csv",
            lambda: pacsv.read_csv(config.csv_path),
            lambda payload: int(payload.num_rows),
        ),
        (
            "pyarrow",
            "parquet",
            lambda: pq.read_table(config.parquet_path),
            lambda payload: int(payload.num_rows),
        ),
    ]

    for _, _, read_func, row_counter in readers:
        warmup_payload = read_func()
        row_counter(warmup_payload)
    gc.collect()

    results: list[ReadBenchmarkResult] = []
    for reader_name, file_format, read_func, row_counter in readers:
        result = measure_reader(
            benchmark_runs=config.benchmark_runs,
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
