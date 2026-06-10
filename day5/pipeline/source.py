"""PostgreSQL source loading helpers for Day 5."""

from __future__ import annotations

import pandas as pd
from sqlalchemy import create_engine, text

from day5.pipeline.config import Day5Config
from day5.pipeline.logging_utils import LOGGER


def build_source_sql(config: Day5Config) -> str:
    """Build source SQL for Day 5 payload generation."""

    limit_sql = ""
    if config.max_papers > 0:
        limit_sql = f"LIMIT {config.max_papers}"

    return f"""
    SELECT
        paper_key,
        title,
        summary,
        primary_category,
        published_at
    FROM {config.source_table}
    ORDER BY {config.order_by}
    {limit_sql}
    """.strip()


def load_source_frame(config: Day5Config) -> pd.DataFrame:
    """Load Day 5 source records from PostgreSQL."""

    engine = create_engine(config.sqlalchemy_url, future=True)
    source_sql = build_source_sql(config)

    LOGGER.info(
        "Loading Day 5 source records",
        extra={
            "context": {
                "source_table": config.source_table,
                "order_by": config.order_by,
                "max_papers": config.max_papers,
            }
        },
    )

    with engine.begin() as connection:
        frame = pd.read_sql_query(text(source_sql), connection)

    if frame.empty:
        raise ValueError("Day 5 source query returned no rows")

    return frame
