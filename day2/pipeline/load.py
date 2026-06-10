"""PostgreSQL table creation and upsert helpers for Day 2."""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Engine

from day2.pipeline.constants import AUTHOR_TABLE, CLEAN_TABLE, RAW_TABLE, REJECT_TABLE, SCHEMA_NAME, STATE_TABLE


def ensure_schema_and_tables(engine: Engine) -> None:
    """Create schema and all Day 2 tables if they do not already exist."""

    create_schema_sql = f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME};"

    create_raw_sql = f"""
    CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.{RAW_TABLE} (
        paper_key TEXT PRIMARY KEY,
        versioned_id TEXT,
        batch_id TEXT NOT NULL,
        page_number INTEGER NOT NULL,
        source_query TEXT NOT NULL,
        source_start_at TIMESTAMPTZ NOT NULL,
        row_hash TEXT NOT NULL UNIQUE,
        raw_payload JSONB NOT NULL,
        ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    """

    create_clean_sql = f"""
    CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.{CLEAN_TABLE} (
        paper_key TEXT PRIMARY KEY,
        versioned_id TEXT NOT NULL,
        batch_id TEXT NOT NULL,
        row_hash TEXT NOT NULL UNIQUE,
        title TEXT NOT NULL,
        summary TEXT NOT NULL,
        published_at TIMESTAMPTZ NOT NULL,
        updated_at TIMESTAMPTZ NOT NULL,
        primary_category TEXT NOT NULL,
        categories_json JSONB NOT NULL,
        author_count INTEGER NOT NULL,
        first_author TEXT NOT NULL,
        authors_preview TEXT NOT NULL,
        arxiv_url TEXT NOT NULL,
        pdf_url TEXT,
        comment TEXT,
        journal_ref TEXT,
        doi TEXT,
        raw_payload JSONB NOT NULL,
        loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    """

    create_author_sql = f"""
    CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.{AUTHOR_TABLE} (
        paper_key TEXT NOT NULL,
        author_order INTEGER NOT NULL,
        author_name TEXT NOT NULL,
        affiliation TEXT,
        batch_id TEXT NOT NULL,
        raw_payload JSONB NOT NULL,
        loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        PRIMARY KEY (paper_key, author_order),
        CONSTRAINT fk_paper_authors_paper_key
            FOREIGN KEY (paper_key)
            REFERENCES {SCHEMA_NAME}.{CLEAN_TABLE} (paper_key)
            ON DELETE CASCADE
    );
    """

    create_reject_sql = f"""
    CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.{REJECT_TABLE} (
        id BIGSERIAL PRIMARY KEY,
        paper_key TEXT,
        batch_id TEXT NOT NULL,
        page_number INTEGER NOT NULL,
        row_hash TEXT NOT NULL UNIQUE,
        reason TEXT NOT NULL,
        raw_payload JSONB NOT NULL,
        rejected_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    """

    drop_legacy_reject_paper_key_unique_sql = f"""
    ALTER TABLE {SCHEMA_NAME}.{REJECT_TABLE}
    DROP CONSTRAINT IF EXISTS {REJECT_TABLE}_paper_key_key;
    """

    create_state_sql = f"""
    CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.{STATE_TABLE} (
        source_name TEXT PRIMARY KEY,
        watermark_published_at TIMESTAMPTZ NOT NULL,
        watermark_paper_key TEXT NOT NULL,
        last_batch_id TEXT,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    """

    with engine.begin() as connection:
        connection.execute(text(create_schema_sql))
        connection.execute(text(create_raw_sql))
        connection.execute(text(create_clean_sql))
        connection.execute(text(create_author_sql))
        connection.execute(text(create_reject_sql))
        connection.execute(text(drop_legacy_reject_paper_key_unique_sql))
        connection.execute(text(create_state_sql))


def upsert_raw_records(
    engine: Engine,
    batch_id: str,
    source_query: str,
    source_start_at: datetime,
    page_number: int,
    entries: list[dict[str, Any]],
) -> None:
    """Idempotently upsert raw page records for auditability and replay."""

    records = []
    for entry in entries:
        records.append(
            {
                "paper_key": entry.get("paper_key"),
                "versioned_id": entry.get("versioned_id"),
                "batch_id": batch_id,
                "page_number": page_number,
                "source_query": source_query,
                "source_start_at": source_start_at,
                "row_hash": entry.get("row_hash"),
                "raw_payload": json.dumps(entry.get("raw_payload", {}), ensure_ascii=True, default=str),
            }
        )

    if not records:
        return

    sql = text(
        f"""
        INSERT INTO {SCHEMA_NAME}.{RAW_TABLE}
            (paper_key, versioned_id, batch_id, page_number, source_query, source_start_at, row_hash, raw_payload)
        VALUES
            (:paper_key, :versioned_id, :batch_id, :page_number, :source_query, :source_start_at, :row_hash, CAST(:raw_payload AS JSONB))
        ON CONFLICT (paper_key)
        DO UPDATE SET
            versioned_id = EXCLUDED.versioned_id,
            batch_id = EXCLUDED.batch_id,
            page_number = EXCLUDED.page_number,
            source_query = EXCLUDED.source_query,
            source_start_at = EXCLUDED.source_start_at,
            row_hash = EXCLUDED.row_hash,
            raw_payload = EXCLUDED.raw_payload,
            ingested_at = NOW();
        """
    )

    with engine.begin() as connection:
        connection.execute(sql, records)


def upsert_clean_records(engine: Engine, batch_id: str, entries: list[dict[str, Any]]) -> None:
    """Idempotently upsert flattened clean paper records."""

    records = []
    for entry in entries:
        records.append(
            {
                "paper_key": entry["paper_key"],
                "versioned_id": entry["versioned_id"],
                "batch_id": batch_id,
                "row_hash": entry["row_hash"],
                "title": entry["title"],
                "summary": entry["summary"],
                "published_at": entry["published_at"],
                "updated_at": entry["updated_at"],
                "primary_category": entry["primary_category"],
                "categories_json": json.dumps(entry["categories"], ensure_ascii=True, default=str),
                "author_count": int(entry["author_count"]),
                "first_author": entry["first_author"],
                "authors_preview": entry["authors_preview"],
                "arxiv_url": entry["arxiv_url"],
                "pdf_url": entry["pdf_url"],
                "comment": entry["comment"],
                "journal_ref": entry["journal_ref"],
                "doi": entry["doi"],
                "raw_payload": json.dumps(entry["raw_payload"], ensure_ascii=True, default=str),
            }
        )

    if not records:
        return

    sql = text(
        f"""
        INSERT INTO {SCHEMA_NAME}.{CLEAN_TABLE}
            (
                paper_key,
                versioned_id,
                batch_id,
                row_hash,
                title,
                summary,
                published_at,
                updated_at,
                primary_category,
                categories_json,
                author_count,
                first_author,
                authors_preview,
                arxiv_url,
                pdf_url,
                comment,
                journal_ref,
                doi,
                raw_payload
            )
        VALUES
            (
                :paper_key,
                :versioned_id,
                :batch_id,
                :row_hash,
                :title,
                :summary,
                :published_at,
                :updated_at,
                :primary_category,
                CAST(:categories_json AS JSONB),
                :author_count,
                :first_author,
                :authors_preview,
                :arxiv_url,
                :pdf_url,
                :comment,
                :journal_ref,
                :doi,
                CAST(:raw_payload AS JSONB)
            )
        ON CONFLICT (paper_key)
        DO UPDATE SET
            versioned_id = EXCLUDED.versioned_id,
            batch_id = EXCLUDED.batch_id,
            row_hash = EXCLUDED.row_hash,
            title = EXCLUDED.title,
            summary = EXCLUDED.summary,
            published_at = EXCLUDED.published_at,
            updated_at = EXCLUDED.updated_at,
            primary_category = EXCLUDED.primary_category,
            categories_json = EXCLUDED.categories_json,
            author_count = EXCLUDED.author_count,
            first_author = EXCLUDED.first_author,
            authors_preview = EXCLUDED.authors_preview,
            arxiv_url = EXCLUDED.arxiv_url,
            pdf_url = EXCLUDED.pdf_url,
            comment = EXCLUDED.comment,
            journal_ref = EXCLUDED.journal_ref,
            doi = EXCLUDED.doi,
            raw_payload = EXCLUDED.raw_payload,
            loaded_at = NOW();
        """
    )

    with engine.begin() as connection:
        connection.execute(sql, records)


def upsert_author_records(engine: Engine, batch_id: str, entries: list[dict[str, Any]]) -> None:
    """Idempotently upsert normalized author rows tied to each paper."""

    records = []
    for entry in entries:
        for author in entry["authors"]:
            author_name = str(author.get("author_name") or "").strip()
            if not author_name:
                continue
            records.append(
                {
                    "paper_key": entry["paper_key"],
                    "author_order": int(author["author_order"]),
                    "author_name": author_name,
                    "affiliation": author.get("affiliation") or None,
                    "batch_id": batch_id,
                    "raw_payload": json.dumps(author, ensure_ascii=True, default=str),
                }
            )

    if not records:
        return

    sql = text(
        f"""
        INSERT INTO {SCHEMA_NAME}.{AUTHOR_TABLE}
            (paper_key, author_order, author_name, affiliation, batch_id, raw_payload)
        VALUES
            (:paper_key, :author_order, :author_name, :affiliation, :batch_id, CAST(:raw_payload AS JSONB))
        ON CONFLICT (paper_key, author_order)
        DO UPDATE SET
            author_name = EXCLUDED.author_name,
            affiliation = EXCLUDED.affiliation,
            batch_id = EXCLUDED.batch_id,
            raw_payload = EXCLUDED.raw_payload,
            loaded_at = NOW();
        """
    )

    with engine.begin() as connection:
        connection.execute(sql, records)


def upsert_rejected_records(
    engine: Engine,
    batch_id: str,
    page_number: int,
    rejected_entries: list[dict[str, Any]],
) -> None:
    """Persist rejected entries with reasons for traceable data quality review."""

    records = []
    for entry in rejected_entries:
        records.append(
            {
                "paper_key": entry.get("paper_key"),
                "batch_id": batch_id,
                "page_number": page_number,
                "row_hash": entry.get("row_hash"),
                "reason": entry.get("reason"),
                "raw_payload": json.dumps(entry.get("raw_payload", {}), ensure_ascii=True, default=str),
            }
        )

    if not records:
        return

    sql = text(
        f"""
        INSERT INTO {SCHEMA_NAME}.{REJECT_TABLE}
            (paper_key, batch_id, page_number, row_hash, reason, raw_payload)
        VALUES
            (:paper_key, :batch_id, :page_number, :row_hash, :reason, CAST(:raw_payload AS JSONB))
        ON CONFLICT (row_hash)
        DO UPDATE SET
            paper_key = EXCLUDED.paper_key,
            batch_id = EXCLUDED.batch_id,
            page_number = EXCLUDED.page_number,
            reason = EXCLUDED.reason,
            raw_payload = EXCLUDED.raw_payload,
            rejected_at = NOW();
        """
    )

    with engine.begin() as connection:
        connection.execute(sql, records)
