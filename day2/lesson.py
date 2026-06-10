"""Teachable Day 2 pipeline entrypoint.

Open this file first in class. It shows how a live API feed becomes clean,
database-backed training data without starting from a thousand-line script.
"""

from __future__ import annotations

import sys
import time
from pathlib import Path
from typing import Any
from uuid import uuid4

from sqlalchemy import create_engine

if __package__ is None or __package__ == "":
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from day2.pipeline.checks import run_post_load_checks
from day2.pipeline.config import load_config
from day2.pipeline.extract import build_search_query, fetch_feed_page
from day2.pipeline.load import (
    ensure_schema_and_tables,
    upsert_author_records,
    upsert_clean_records,
    upsert_raw_records,
    upsert_rejected_records,
)
from day2.pipeline.logging_utils import LOGGER
from day2.pipeline.outputs import export_rejected_csv_sample
from day2.pipeline.state import resolve_ingestion_window, update_state_from_latest_entry
from day2.pipeline.transform import split_clean_and_reject


def run_pipeline() -> None:
    """Run Day 2 as a readable live-API ingestion story."""

    config = load_config()
    batch_id = str(uuid4())
    engine = create_engine(config.sqlalchemy_url, future=True)

    ensure_schema_and_tables(engine)
    _, source_start_at, source_end_at = resolve_ingestion_window(engine, config)
    search_query = build_search_query(config.arxiv_search_query, source_start_at, source_end_at)

    LOGGER.info(
        "Pipeline started",
        extra={
            "context": {
                "batch_id": batch_id,
                "source_name": config.arxiv_source_name,
                "category_count": len(config.arxiv_categories),
                "categories": config.arxiv_categories,
                "search_query": search_query,
                "source_start_at": source_start_at.isoformat(),
                "source_end_at": source_end_at.isoformat(),
            }
        },
    )

    collected_entries: list[dict[str, Any]] = []
    rejected_entries: list[dict[str, Any]] = []
    page_number = 0
    start = 0
    total_results = None
    latest_seen_entry: dict[str, Any] | None = None

    while len(collected_entries) < config.arxiv_max_papers:
        remaining = config.arxiv_max_papers - len(collected_entries)
        page_size = min(config.arxiv_page_size, remaining)
        page_number += 1

        try:
            page_total, page_entries = fetch_feed_page(config, search_query, start, page_size)
        except Exception as exc:
            LOGGER.error(
                "Stopping pagination after repeated page fetch failures",
                extra={
                    "context": {
                        "batch_id": batch_id,
                        "page_number": page_number,
                        "start": start,
                        "page_size": page_size,
                        "error_type": type(exc).__name__,
                        "error": str(exc),
                    }
                },
            )
            break

        total_results = page_total
        if not page_entries:
            break

        latest_seen_entry = page_entries[-1]
        clean_entries, page_rejected = split_clean_and_reject(page_entries)
        collected_entries.extend(clean_entries)
        rejected_entries.extend(page_rejected)

        upsert_raw_records(engine, batch_id, search_query, source_start_at, page_number, page_entries)
        upsert_clean_records(engine, batch_id, clean_entries)
        upsert_author_records(engine, batch_id, clean_entries)
        upsert_rejected_records(engine, batch_id, page_number, page_rejected)

        if len(page_entries) < page_size:
            break

        start += page_size
        if len(collected_entries) < config.arxiv_max_papers:
            time.sleep(config.arxiv_sleep_seconds)

    update_state_from_latest_entry(engine, config, latest_seen_entry, batch_id)
    rejected_sample_path = export_rejected_csv_sample(rejected_entries, batch_id)
    run_post_load_checks(engine, batch_id)

    LOGGER.info(
        "Pipeline finished successfully",
        extra={
            "context": {
                "batch_id": batch_id,
                "total_results_reported": total_results,
                "clean_rows": len(collected_entries),
                "rejected_rows": len(rejected_entries),
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
