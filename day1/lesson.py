"""Teachable Day 1 pipeline entrypoint.

Open this file first in class. It shows the Day 1 flow without burying learners
inside logging, SQL DDL, validation details, and helper functions.
"""

from __future__ import annotations

import sys
from pathlib import Path
from uuid import uuid4

from sqlalchemy import create_engine

if __package__ is None or __package__ == "":
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from day1.pipeline.checks import run_post_load_checks
from day1.pipeline.config import load_config
from day1.pipeline.extract import download_reviews_csv, read_reviews_csv
from day1.pipeline.load import (
    ensure_schema_and_tables,
    upsert_clean_records,
    upsert_raw_records,
    upsert_rejected_records,
)
from day1.pipeline.logging_utils import LOGGER
from day1.pipeline.outputs import export_rejected_csv_sample
from day1.pipeline.transform import clean_dataset_defensively, prepare_raw_records_for_load


def run_pipeline() -> None:
    """Run Day 1 as a readable extract-transform-load-check story."""

    config = load_config()
    batch_id = str(uuid4())
    repo_root = Path(__file__).resolve().parents[1]

    LOGGER.info(
        "Pipeline started",
        extra={"context": {"batch_id": batch_id, "source_url": config.hf_csv_url}},
    )

    resolved_source_url, csv_path = download_reviews_csv(config, batch_id, repo_root)
    raw_reviews = read_reviews_csv(csv_path)
    if raw_reviews.empty:
        raise ValueError("Downloaded CSV is empty. Stopping pipeline.")

    raw_reviews_for_load = prepare_raw_records_for_load(raw_reviews)
    clean_reviews, rejected_reviews = clean_dataset_defensively(raw_reviews)

    engine = create_engine(config.sqlalchemy_url, future=True)
    ensure_schema_and_tables(engine)
    upsert_raw_records(engine, batch_id, resolved_source_url, raw_reviews_for_load)
    upsert_clean_records(engine, batch_id, clean_reviews)
    upsert_rejected_records(engine, batch_id, rejected_reviews)

    rejected_sample_path = export_rejected_csv_sample(rejected_reviews, batch_id)
    run_post_load_checks(engine, batch_id)

    LOGGER.info(
        "Pipeline finished successfully",
        extra={
            "context": {
                "batch_id": batch_id,
                "raw_rows": int(len(raw_reviews)),
                "clean_rows": int(len(clean_reviews)),
                "rejected_rows": int(len(rejected_reviews)),
                "rejected_sample_csv": str(rejected_sample_path),
            }
        },
    )


def main() -> None:
    """Entrypoint with structured failure logging."""

    try:
        run_pipeline()
    except Exception as exc:
        LOGGER.exception(
            "Pipeline failed",
            extra={"context": {"error_type": type(exc).__name__, "error": str(exc)}},
        )
        raise


if __name__ == "__main__":
    main()
