"""Report building and display helpers for Day 3."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

import pandas as pd

from day3.pipeline.config import Day3Config
from day3.pipeline.logging_utils import LOGGER
from day3.pipeline.models import FileArtifact, ReadBenchmarkResult


def build_summary_rows(
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


def build_report(
    config: Day3Config,
    frame: pd.DataFrame,
    csv_artifact: FileArtifact,
    parquet_artifact: FileArtifact,
    results: list[ReadBenchmarkResult],
) -> dict[str, Any]:
    """Build the structured JSON report for the Day 3 benchmark."""

    summary_rows = build_summary_rows(csv_artifact, parquet_artifact, results)
    benchmark_rows = [result.__dict__ for result in results]

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source": {
            "table": config.source_table,
            "order_by": config.order_by,
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
    summary_frame = pd.DataFrame(summary_rows)
    print("\nDay 3 Benchmark Summary")
    print(summary_frame.to_string(index=False))
    print()
