"""Teachable Day 3 benchmark pipeline entrypoint.

Open this file first in class. It shows the Day 3 story without burying learners
inside benchmark timing details or PyArrow internals.
"""

from __future__ import annotations

import sys
from pathlib import Path

from sqlalchemy import create_engine

if __package__ is None or __package__ == "":
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from day3.pipeline.benchmark import run_benchmarks
from day3.pipeline.config import load_config
from day3.pipeline.exports import write_exports
from day3.pipeline.report import build_report, print_summary_table, write_report
from day3.pipeline.source import load_source_frame, prepare_export_frame


def run_pipeline() -> None:
    """Run Day 3 as a readable export-benchmark-report story."""

    config = load_config()
    config.output_dir.mkdir(parents=True, exist_ok=True)

    engine = create_engine(config.sqlalchemy_url, future=True)
    source_frame = load_source_frame(engine, config)
    export_frame = prepare_export_frame(source_frame)

    csv_artifact, parquet_artifact = write_exports(config, export_frame)
    results = run_benchmarks(config)

    report = build_report(config, export_frame, csv_artifact, parquet_artifact, results)
    write_report(config, report)
    print_summary_table(csv_artifact, parquet_artifact, results)


def main() -> None:
    """Entry point for the Day 3 benchmark pipeline."""

    run_pipeline()


if __name__ == "__main__":
    main()
