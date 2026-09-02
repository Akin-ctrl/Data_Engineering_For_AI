"""Report building and display helpers for Day 3."""

from __future__ import annotations

import json
from dataclasses import asdict
from datetime import datetime, timezone
from typing import Any

import pandas as pd

from day3.pipeline.config import Day3Config
from day3.pipeline.constants import PROJECTION_COLUMNS, READ_MODES
from day3.pipeline.logging_utils import LOGGER
from day3.pipeline.models import FileArtifact, ReadBenchmarkResult

READER_NAMES = ["pandas", "pyarrow"]


def group_results(
    results: list[ReadBenchmarkResult],
) -> dict[tuple[str, str], dict[str, ReadBenchmarkResult]]:
    """Group benchmark results by reader and read mode, then by file format.

    Args:
        results: Every timing result produced by the benchmark.

    Returns:
        A lookup keyed by (reader, read_mode), holding the csv and parquet
        result for that combination.
    """

    grouped: dict[tuple[str, str], dict[str, ReadBenchmarkResult]] = {}
    for result in results:
        key = (result.reader, result.read_mode)
        grouped.setdefault(key, {})[result.file_format] = result

    return grouped


def build_summary_rows(
    csv_artifact: FileArtifact,
    parquet_artifact: FileArtifact,
    results: list[ReadBenchmarkResult],
) -> list[dict[str, Any]]:
    """Create the classroom-friendly comparison rows.

    One row per reader and read mode, so learners can see the full read and the
    projected read side by side.
    """

    grouped = group_results(results)

    size_delta_mb = csv_artifact.size_mb - parquet_artifact.size_mb
    size_reduction_pct = (
        (size_delta_mb / csv_artifact.size_mb) * 100 if csv_artifact.size_mb else 0.0
    )

    summary_rows: list[dict[str, Any]] = []
    for read_mode in READ_MODES:
        for reader_name in READER_NAMES:
            formats = grouped.get((reader_name, read_mode), {})
            csv_result = formats.get("csv")
            parquet_result = formats.get("parquet")
            if csv_result is None or parquet_result is None:
                continue

            ms_delta = csv_result.average_ms - parquet_result.average_ms
            # A speedup above 1 means Parquet read faster than CSV.
            speedup_factor = (
                csv_result.average_ms / parquet_result.average_ms
                if parquet_result.average_ms
                else 0.0
            )

            summary_rows.append(
                {
                    "read_mode": read_mode,
                    "reader": reader_name,
                    "columns": csv_result.columns_read,
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


def build_report(
    config: Day3Config,
    frame: pd.DataFrame,
    csv_artifact: FileArtifact,
    parquet_artifact: FileArtifact,
    results: list[ReadBenchmarkResult],
) -> dict[str, Any]:
    """Build the structured JSON report for the Day 3 benchmark."""

    summary_rows = build_summary_rows(csv_artifact, parquet_artifact, results)
    benchmark_rows = [asdict(result) for result in results]

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source": {
            "table": config.source_table,
            "order_by": config.order_by,
            "rows": int(len(frame)),
            "columns": list(frame.columns),
        },
        "projection_columns": list(PROJECTION_COLUMNS),
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


def write_report(config: Day3Config, report: dict[str, Any]) -> None:
    """Persist the benchmark report as JSON."""

    config.report_path.write_text(
        json.dumps(report, indent=2, ensure_ascii=True, default=str),
        encoding="utf-8",
    )
    LOGGER.info(
        "Benchmark report written",
        extra={
            "context": {
                "report_path": str(config.report_path),
            }
        },
    )


def print_summary_table(
    csv_artifact: FileArtifact,
    parquet_artifact: FileArtifact,
    results: list[ReadBenchmarkResult],
) -> None:
    """Print a concise comparison table for the classroom."""

    summary_rows = build_summary_rows(csv_artifact, parquet_artifact, results)
    if not summary_rows:
        print("\nDay 3 Benchmark Summary: no results to show\n")
        return

    timing_columns = [
        "read_mode",
        "reader",
        "columns",
        "csv_avg_ms",
        "parquet_avg_ms",
        "ms_delta",
        "speedup_factor",
    ]
    summary_frame = pd.DataFrame(summary_rows)[timing_columns]

    first_row = summary_rows[0]
    print("\nDay 3 Benchmark Summary")
    print(summary_frame.to_string(index=False))
    print(
        f"\nFile size: CSV {first_row['csv_size_mb']} MB, "
        f"Parquet {first_row['parquet_size_mb']} MB, "
        f"{first_row['size_reduction_pct']}% smaller."
    )
    print(f"Projected reads pull back only: {', '.join(PROJECTION_COLUMNS)}")
    print("A speedup_factor above 1 means Parquet read faster than CSV.\n")
