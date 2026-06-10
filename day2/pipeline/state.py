"""Watermark state helpers for the Day 2 pipeline."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Engine

from day2.pipeline.config import PipelineConfig
from day2.pipeline.constants import SCHEMA_NAME, STATE_TABLE


def get_state(engine: Engine, source_name: str) -> dict[str, Any] | None:
    """Read the persisted watermark row for a specific source name."""

    sql = text(
        f"""
        SELECT source_name, watermark_published_at, watermark_paper_key, last_batch_id
        FROM {SCHEMA_NAME}.{STATE_TABLE}
        WHERE source_name = :source_name
        """
    )

    with engine.begin() as connection:
        row = connection.execute(sql, {"source_name": source_name}).mappings().first()

    return dict(row) if row else None


def upsert_state(
    engine: Engine,
    source_name: str,
    watermark_published_at: datetime,
    watermark_paper_key: str,
    batch_id: str,
) -> None:
    """Insert or update ingestion watermark to support incremental pagination."""

    sql = text(
        f"""
        INSERT INTO {SCHEMA_NAME}.{STATE_TABLE}
            (source_name, watermark_published_at, watermark_paper_key, last_batch_id)
        VALUES
            (:source_name, :watermark_published_at, :watermark_paper_key, :last_batch_id)
        ON CONFLICT (source_name)
        DO UPDATE SET
            watermark_published_at = EXCLUDED.watermark_published_at,
            watermark_paper_key = EXCLUDED.watermark_paper_key,
            last_batch_id = EXCLUDED.last_batch_id,
            updated_at = NOW();
        """
    )

    with engine.begin() as connection:
        connection.execute(
            sql,
            {
                "source_name": source_name,
                "watermark_published_at": watermark_published_at,
                "watermark_paper_key": watermark_paper_key,
                "last_batch_id": batch_id,
            },
        )


def resolve_ingestion_window(
    engine: Engine,
    config: PipelineConfig,
) -> tuple[dict[str, Any] | None, datetime, datetime]:
    """Resolve start/end timestamps for the current ingestion run."""

    current_state = get_state(engine, config.arxiv_source_name)
    now = datetime.now(timezone.utc)
    if current_state:
        source_start_at = current_state["watermark_published_at"] - timedelta(minutes=config.arxiv_overlap_minutes)
    else:
        source_start_at = now - timedelta(days=config.arxiv_default_lookback_days)

    return current_state, source_start_at, now


def update_state_from_latest_entry(
    engine: Engine,
    config: PipelineConfig,
    latest_seen_entry: dict[str, Any] | None,
    batch_id: str,
) -> None:
    """Persist the latest successfully seen paper as the next watermark."""

    if latest_seen_entry and latest_seen_entry.get("published_at") is not None:
        upsert_state(
            engine,
            config.arxiv_source_name,
            latest_seen_entry["published_at"],
            latest_seen_entry["paper_key"],
            batch_id,
        )
