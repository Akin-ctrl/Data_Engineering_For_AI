"""PostgreSQL table creation and loading helpers for Day 1."""

from __future__ import annotations

import json

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine


SCHEMA_NAME = "de_ai"
RAW_TABLE = "raw_reviews"
CLEAN_TABLE = "clean_reviews"
REJECT_TABLE = "rejected_reviews"


def ensure_schema_and_tables(engine: Engine) -> None:
    """Create schema and target tables for raw, clean, and rejected records."""

    create_schema_sql = f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME};"

    create_raw_sql = f"""
    CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.{RAW_TABLE} (
        id BIGSERIAL PRIMARY KEY,
        batch_id TEXT NOT NULL,
        row_hash TEXT NOT NULL UNIQUE,
        row_number INTEGER NOT NULL,
        source_url TEXT NOT NULL,
        row_data JSONB NOT NULL,
        ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    """

    create_clean_sql = f"""
    CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.{CLEAN_TABLE} (
        review_key TEXT PRIMARY KEY,
        batch_id TEXT NOT NULL,
        row_hash TEXT NOT NULL UNIQUE,
        review_id BIGINT,
        clothing_id INTEGER NOT NULL,
        age INTEGER,
        title TEXT,
        review_text TEXT NOT NULL,
        rating INTEGER NOT NULL,
        recommended_ind INTEGER NOT NULL,
        positive_feedback_count INTEGER NOT NULL,
        division_name TEXT,
        department_name TEXT,
        class_name TEXT,
        raw_payload JSONB NOT NULL,
        cleaned_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    """

    create_reject_sql = f"""
    CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.{REJECT_TABLE} (
        id BIGSERIAL PRIMARY KEY,
        batch_id TEXT NOT NULL,
        row_hash TEXT NOT NULL UNIQUE,
        row_number INTEGER NOT NULL,
        reason TEXT NOT NULL,
        raw_payload JSONB NOT NULL,
        rejected_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    """

    with engine.begin() as connection:
        connection.execute(text(create_schema_sql))
        connection.execute(text(create_raw_sql))
        connection.execute(text(create_clean_sql))
        connection.execute(text(create_reject_sql))


def upsert_raw_records(
    engine: Engine,
    batch_id: str,
    source_url: str,
    df_normalized: pd.DataFrame,
) -> None:
    """Insert or update all raw rows into the raw table using row_hash conflict key."""

    records = []
    for _, row in df_normalized.iterrows():
        payload = {
            key: (None if pd.isna(value) else str(value))
            for key, value in row.to_dict().items()
        }
        records.append(
            {
                "batch_id": batch_id,
                "row_hash": row["row_hash"],
                "row_number": int(row["row_number"]),
                "source_url": source_url,
                "row_data": json.dumps(payload, ensure_ascii=True),
            }
        )

    sql = text(
        f"""
        INSERT INTO {SCHEMA_NAME}.{RAW_TABLE}
            (batch_id, row_hash, row_number, source_url, row_data)
        VALUES
            (:batch_id, :row_hash, :row_number, :source_url, CAST(:row_data AS JSONB))
        ON CONFLICT (row_hash)
        DO UPDATE SET
            batch_id = EXCLUDED.batch_id,
            row_number = EXCLUDED.row_number,
            source_url = EXCLUDED.source_url,
            row_data = EXCLUDED.row_data,
            ingested_at = NOW();
        """
    )

    with engine.begin() as connection:
        connection.execute(sql, records)


def upsert_clean_records(engine: Engine, batch_id: str, df_clean: pd.DataFrame) -> None:
    """Insert or update clean rows into structured clean table."""

    records = []
    for _, row in df_clean.iterrows():
        records.append(
            {
                "review_key": row["review_key"],
                "batch_id": batch_id,
                "row_hash": row["row_hash"],
                "review_id": None if pd.isna(row["review_id"]) else int(row["review_id"]),
                "clothing_id": int(row["clothing_id"]),
                "age": None if pd.isna(row["age"]) else int(row["age"]),
                "title": None if str(row["title"]).strip().lower() in {"", "nan", "none"} else str(row["title"]).strip(),
                "review_text": str(row["review_text"]).strip(),
                "rating": int(row["rating"]),
                "recommended_ind": int(row["recommended_ind"]),
                "positive_feedback_count": int(row["positive_feedback_count"]),
                "division_name": None if str(row["division_name"]).strip().lower() in {"", "nan", "none"} else str(row["division_name"]).strip(),
                "department_name": None if str(row["department_name"]).strip().lower() in {"", "nan", "none"} else str(row["department_name"]).strip(),
                "class_name": None if str(row["class_name"]).strip().lower() in {"", "nan", "none"} else str(row["class_name"]).strip(),
                "raw_payload": json.dumps(row["raw_payload"], ensure_ascii=True),
            }
        )

    if not records:
        return

    sql = text(
        f"""
        INSERT INTO {SCHEMA_NAME}.{CLEAN_TABLE}
            (
                review_key,
                batch_id,
                row_hash,
                review_id,
                clothing_id,
                age,
                title,
                review_text,
                rating,
                recommended_ind,
                positive_feedback_count,
                division_name,
                department_name,
                class_name,
                raw_payload
            )
        VALUES
            (
                :review_key,
                :batch_id,
                :row_hash,
                :review_id,
                :clothing_id,
                :age,
                :title,
                :review_text,
                :rating,
                :recommended_ind,
                :positive_feedback_count,
                :division_name,
                :department_name,
                :class_name,
                CAST(:raw_payload AS JSONB)
            )
        ON CONFLICT (review_key)
        DO UPDATE SET
            batch_id = EXCLUDED.batch_id,
            row_hash = EXCLUDED.row_hash,
            review_id = EXCLUDED.review_id,
            clothing_id = EXCLUDED.clothing_id,
            age = EXCLUDED.age,
            title = EXCLUDED.title,
            review_text = EXCLUDED.review_text,
            rating = EXCLUDED.rating,
            recommended_ind = EXCLUDED.recommended_ind,
            positive_feedback_count = EXCLUDED.positive_feedback_count,
            division_name = EXCLUDED.division_name,
            department_name = EXCLUDED.department_name,
            class_name = EXCLUDED.class_name,
            raw_payload = EXCLUDED.raw_payload,
            cleaned_at = NOW();
        """
    )

    with engine.begin() as connection:
        connection.execute(sql, records)


def upsert_rejected_records(engine: Engine, batch_id: str, df_reject: pd.DataFrame) -> None:
    """Insert or update rejected rows and reasons into reject table."""

    records = []
    for _, row in df_reject.iterrows():
        records.append(
            {
                "batch_id": batch_id,
                "row_hash": row["row_hash"],
                "row_number": int(row["row_number"]),
                "reason": row["reject_reason"],
                "raw_payload": json.dumps(row["raw_payload"], ensure_ascii=True),
            }
        )

    if not records:
        return

    sql = text(
        f"""
        INSERT INTO {SCHEMA_NAME}.{REJECT_TABLE}
            (batch_id, row_hash, row_number, reason, raw_payload)
        VALUES
            (:batch_id, :row_hash, :row_number, :reason, CAST(:raw_payload AS JSONB))
        ON CONFLICT (row_hash)
        DO UPDATE SET
            batch_id = EXCLUDED.batch_id,
            row_number = EXCLUDED.row_number,
            reason = EXCLUDED.reason,
            raw_payload = EXCLUDED.raw_payload,
            rejected_at = NOW();
        """
    )

    with engine.begin() as connection:
        connection.execute(sql, records)
