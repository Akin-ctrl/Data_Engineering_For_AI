"""Source query helpers for Day 3 exports."""

from __future__ import annotations

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

from day3.pipeline.config import Day3Config
from day3.pipeline.logging_utils import LOGGER


def build_source_sql(config: Day3Config) -> str:
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
    FROM {config.source_table}
    ORDER BY {config.order_by}
    """.strip()


def load_source_frame(engine: Engine, config: Day3Config) -> pd.DataFrame:
    """Load the clean Day 2 table from PostgreSQL into a pandas DataFrame."""

    source_sql = build_source_sql(config)
    LOGGER.info(
        "Loading source data from PostgreSQL",
        extra={
            "context": {
                "source_table": config.source_table,
                "order_by": config.order_by,
            }
        },
    )

    with engine.begin() as connection:
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


def prepare_export_frame(frame: pd.DataFrame) -> pd.DataFrame:
    """Create a stable export frame suitable for both CSV and Parquet."""

    return frame.copy()
