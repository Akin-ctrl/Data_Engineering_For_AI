"""Post-load quality checks for Day 2."""

from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.engine import Engine

from day2.pipeline.constants import AUTHOR_TABLE, CLEAN_TABLE, RAW_TABLE, REJECT_TABLE, SCHEMA_NAME
from day2.pipeline.logging_utils import LOGGER


def run_post_load_checks(engine: Engine, batch_id: str) -> None:
    """Run quick sanity checks and log ingestion quality metrics."""

    checks = {
        "raw_count": text(f"SELECT COUNT(*) FROM {SCHEMA_NAME}.{RAW_TABLE} WHERE batch_id = :batch_id"),
        "clean_count": text(f"SELECT COUNT(*) FROM {SCHEMA_NAME}.{CLEAN_TABLE} WHERE batch_id = :batch_id"),
        "author_count": text(f"SELECT COUNT(*) FROM {SCHEMA_NAME}.{AUTHOR_TABLE} WHERE batch_id = :batch_id"),
        "reject_count": text(f"SELECT COUNT(*) FROM {SCHEMA_NAME}.{REJECT_TABLE} WHERE batch_id = :batch_id"),
        "clean_nulls": text(
            f"""
            SELECT COUNT(*)
            FROM {SCHEMA_NAME}.{CLEAN_TABLE}
            WHERE batch_id = :batch_id
              AND (
                    paper_key IS NULL
                 OR title IS NULL
                 OR summary IS NULL
                 OR published_at IS NULL
                 OR updated_at IS NULL
                 OR primary_category IS NULL
                 OR author_count IS NULL
                 OR first_author IS NULL
              )
            """
        ),
        "author_duplicates": text(
            f"""
            SELECT COALESCE(SUM(cnt) - COUNT(*), 0)
            FROM (
                SELECT paper_key, author_order, COUNT(*) AS cnt
                FROM {SCHEMA_NAME}.{AUTHOR_TABLE}
                WHERE batch_id = :batch_id
                GROUP BY paper_key, author_order
            ) grouped
            """
        ),
    }

    with engine.begin() as connection:
        raw_count = connection.execute(checks["raw_count"], {"batch_id": batch_id}).scalar_one()
        clean_count = connection.execute(checks["clean_count"], {"batch_id": batch_id}).scalar_one()
        author_count = connection.execute(checks["author_count"], {"batch_id": batch_id}).scalar_one()
        reject_count = connection.execute(checks["reject_count"], {"batch_id": batch_id}).scalar_one()
        clean_nulls = connection.execute(checks["clean_nulls"], {"batch_id": batch_id}).scalar_one()
        author_duplicates = connection.execute(checks["author_duplicates"], {"batch_id": batch_id}).scalar_one()

    LOGGER.info(
        "Post-load checks completed",
        extra={
            "context": {
                "batch_id": batch_id,
                "raw_count": raw_count,
                "clean_count": clean_count,
                "author_count": author_count,
                "reject_count": reject_count,
                "clean_null_violations": clean_nulls,
                "author_duplicate_key_rows": author_duplicates,
            }
        },
    )
