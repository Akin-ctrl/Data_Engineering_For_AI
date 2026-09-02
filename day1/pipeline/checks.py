"""Post-load quality checks for Day 1."""

from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.engine import Engine

from day1.pipeline.constants import CLEAN_TABLE, RAW_TABLE, REJECT_TABLE, SCHEMA_NAME
from day1.pipeline.logging_utils import LOGGER


def run_post_load_checks(engine: Engine, batch_id: str) -> None:
    """Run baseline quality checks after loading and log results."""

    checks = {
        "raw_count": text(
            f"SELECT COUNT(*) FROM {SCHEMA_NAME}.{RAW_TABLE} WHERE batch_id = :batch_id"
        ),
        "clean_count": text(
            f"SELECT COUNT(*) FROM {SCHEMA_NAME}.{CLEAN_TABLE} WHERE batch_id = :batch_id"
        ),
        "reject_count": text(
            f"SELECT COUNT(*) FROM {SCHEMA_NAME}.{REJECT_TABLE} WHERE batch_id = :batch_id"
        ),
        "clean_nulls": text(
            f"""
            SELECT COUNT(*)
            FROM {SCHEMA_NAME}.{CLEAN_TABLE}
            WHERE batch_id = :batch_id
              AND (
                    review_key IS NULL
                 OR clothing_id IS NULL
                 OR review_text IS NULL
                 OR rating IS NULL
                 OR recommended_ind IS NULL
              )
            """
        ),
        "clean_duplicate_keys": text(
            f"""
            SELECT COALESCE(SUM(cnt) - COUNT(*), 0)
            FROM (
                SELECT review_key, COUNT(*) AS cnt
                FROM {SCHEMA_NAME}.{CLEAN_TABLE}
                WHERE batch_id = :batch_id
                GROUP BY review_key
            ) grouped
            """
        ),
        "sample_events": text(
            f"""
            SELECT rating, COUNT(*) AS rating_count
            FROM {SCHEMA_NAME}.{CLEAN_TABLE}
            WHERE batch_id = :batch_id
            GROUP BY rating
            ORDER BY rating ASC
            LIMIT 5
            """
        ),
    }

    with engine.begin() as connection:
        raw_count = connection.execute(checks["raw_count"], {"batch_id": batch_id}).scalar_one()
        clean_count = connection.execute(checks["clean_count"], {"batch_id": batch_id}).scalar_one()
        reject_count = connection.execute(checks["reject_count"], {"batch_id": batch_id}).scalar_one()
        clean_nulls = connection.execute(
            checks["clean_nulls"], {"batch_id": batch_id}
        ).scalar_one()
        duplicate_keys = connection.execute(
            checks["clean_duplicate_keys"], {"batch_id": batch_id}
        ).scalar_one()
        sample_rows = connection.execute(
            checks["sample_events"], {"batch_id": batch_id}
        ).mappings().all()

    LOGGER.info(
        "Post-load checks completed",
        extra={
            "context": {
                "batch_id": batch_id,
                "raw_count": raw_count,
                "clean_count": clean_count,
                "reject_count": reject_count,
                "clean_null_violations": clean_nulls,
                "clean_duplicate_key_rows": duplicate_keys,
                "rating_distribution_sample": [dict(row) for row in sample_rows],
            }
        },
    )
