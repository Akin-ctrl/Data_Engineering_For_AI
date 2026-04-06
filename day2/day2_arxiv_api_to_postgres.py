"""Day 2 pipeline: pull ArXiv papers, flatten the feed, and load PostgreSQL idempotently.

The live ArXiv API returns Atom/XML, so this script parses each entry into a nested
Python structure, flattens the paper-level fields into an analytical table, and stores
repeated authors in a normalized child table.
"""

from __future__ import annotations

import csv
import hashlib
import json
import logging
import os
import re
import time
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

import requests
from dotenv import load_dotenv
from requests import Response
from requests.exceptions import RequestException
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine


SCHEMA_NAME = "training_data"
RAW_TABLE = "raw_arxiv_entries"
CLEAN_TABLE = "clean_papers"
AUTHOR_TABLE = "paper_authors"
REJECT_TABLE = "rejected_arxiv_entries"
STATE_TABLE = "pipeline_state"

ARXIV_NS = "http://www.w3.org/2005/Atom"
OPENSEARCH_NS = "http://a9.com/-/spec/opensearch/1.1/"
ARXIV_META_NS = "http://arxiv.org/schemas/atom"
NAMESPACES = {
    "atom": ARXIV_NS,
    "opensearch": OPENSEARCH_NS,
    "arxiv": ARXIV_META_NS,
}

DEFAULT_ARXIV_CATEGORIES = [
    "cs.LG",
    "cs.AI",
    "cs.CV",
    "cs.CL",
    "stat.ML",
    "cs.RO",
    "cs.IR",
    "cs.CR",
]


class JsonFormatter(logging.Formatter):
    """Format logs as structured JSON for reliable downstream parsing."""

    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
        }

        if hasattr(record, "context"):
            payload["context"] = getattr(record, "context")

        return json.dumps(payload, ensure_ascii=True, default=str)


def configure_logger() -> logging.Logger:
    """Create and configure the pipeline logger exactly once."""

    logger = logging.getLogger("day2_pipeline")
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(JsonFormatter())
        logger.addHandler(handler)

    return logger


LOGGER = configure_logger()


@dataclass(frozen=True)
class PipelineConfig:
    """Typed configuration for the Day 2 ArXiv ingestion pipeline."""

    arxiv_base_url: str
    arxiv_search_query: str
    arxiv_categories: list[str]
    arxiv_source_name: str
    arxiv_max_papers: int
    arxiv_page_size: int
    arxiv_sleep_seconds: int
    arxiv_default_lookback_days: int
    arxiv_overlap_minutes: int
    arxiv_request_timeout_seconds: int
    arxiv_request_max_retries: int
    arxiv_retry_backoff_seconds: int
    arxiv_user_agent: str
    pghost: str
    pgport: int
    pgdatabase: str
    pguser: str
    pgpassword: str

    @property
    def sqlalchemy_url(self) -> str:
        """Build SQLAlchemy URL from environment-driven PostgreSQL settings."""

        return (
            f"postgresql+psycopg://{self.pguser}:{self.pgpassword}"
            f"@{self.pghost}:{self.pgport}/{self.pgdatabase}"
        )


def parse_csv_list(value: str | None) -> list[str]:
    """Parse comma-separated config values into a clean, deterministic list."""

    if not value:
        return []
    return [item.strip() for item in value.split(",") if item.strip()]


def parse_positive_int(value: str, env_name: str) -> int:
    """Parse and validate positive integer environment values."""

    try:
        parsed = int(value)
    except ValueError as exc:  # pragma: no cover - defensive branch
        raise ValueError(f"{env_name} must be an integer, got: {value}") from exc

    if parsed <= 0:
        raise ValueError(f"{env_name} must be greater than zero, got: {parsed}")

    return parsed


def load_config() -> PipelineConfig:
    """Load and validate pipeline configuration from the environment."""

    load_dotenv()

    required = [
        "ARXIV_BASE_URL",
        "ARXIV_SOURCE_NAME",
        "ARXIV_MAX_PAPERS",
        "ARXIV_PAGE_SIZE",
        "ARXIV_SLEEP_SECONDS",
        "ARXIV_DEFAULT_LOOKBACK_DAYS",
        "ARXIV_OVERLAP_MINUTES",
        "PGHOST",
        "PGPORT",
        "PGDATABASE",
        "PGUSER",
        "PGPASSWORD",
    ]

    missing = [key for key in required if not os.getenv(key)]
    if missing:
        raise ValueError(f"Missing required environment variables: {', '.join(missing)}")

    categories = parse_csv_list(os.getenv("ARXIV_CATEGORIES"))
    if not categories:
        categories = DEFAULT_ARXIV_CATEGORIES.copy()

    search_query = os.getenv("ARXIV_SEARCH_QUERY", "").strip()
    if not search_query:
        search_query = " OR ".join(f"cat:{category}" for category in categories)

    arxiv_max_papers = parse_positive_int(os.environ["ARXIV_MAX_PAPERS"], "ARXIV_MAX_PAPERS")
    if arxiv_max_papers < 10000:
        LOGGER.warning(
            "ARXIV_MAX_PAPERS is below the Day 2 deep-learning target",
            extra={"context": {"arxiv_max_papers": arxiv_max_papers, "recommended_min": 10000}},
        )

    arxiv_page_size = parse_positive_int(os.environ["ARXIV_PAGE_SIZE"], "ARXIV_PAGE_SIZE")
    arxiv_sleep_seconds = parse_positive_int(os.environ["ARXIV_SLEEP_SECONDS"], "ARXIV_SLEEP_SECONDS")
    arxiv_default_lookback_days = parse_positive_int(
        os.environ["ARXIV_DEFAULT_LOOKBACK_DAYS"],
        "ARXIV_DEFAULT_LOOKBACK_DAYS",
    )
    arxiv_overlap_minutes = parse_positive_int(
        os.environ["ARXIV_OVERLAP_MINUTES"],
        "ARXIV_OVERLAP_MINUTES",
    )
    arxiv_request_timeout_seconds = parse_positive_int(
        os.getenv("ARXIV_REQUEST_TIMEOUT_SECONDS", "90"),
        "ARXIV_REQUEST_TIMEOUT_SECONDS",
    )
    arxiv_request_max_retries = parse_positive_int(
        os.getenv("ARXIV_REQUEST_MAX_RETRIES", "4"),
        "ARXIV_REQUEST_MAX_RETRIES",
    )
    arxiv_retry_backoff_seconds = parse_positive_int(
        os.getenv("ARXIV_RETRY_BACKOFF_SECONDS", "5"),
        "ARXIV_RETRY_BACKOFF_SECONDS",
    )
    arxiv_user_agent = os.getenv(
        "ARXIV_USER_AGENT",
        "DataEngineeringForAI-Day2/1.0 (contact: instructor@example.com)",
    ).strip()
    if not arxiv_user_agent:
        raise ValueError("ARXIV_USER_AGENT must not be empty")

    if arxiv_sleep_seconds < 3:
        LOGGER.warning(
            "ARXIV_SLEEP_SECONDS is below the recommended polite threshold",
            extra={"context": {"arxiv_sleep_seconds": arxiv_sleep_seconds, "recommended_min": 3}},
        )

    return PipelineConfig(
        arxiv_base_url=os.environ["ARXIV_BASE_URL"],
        arxiv_search_query=search_query,
        arxiv_categories=categories,
        arxiv_source_name=os.environ["ARXIV_SOURCE_NAME"],
        arxiv_max_papers=arxiv_max_papers,
        arxiv_page_size=arxiv_page_size,
        arxiv_sleep_seconds=arxiv_sleep_seconds,
        arxiv_default_lookback_days=arxiv_default_lookback_days,
        arxiv_overlap_minutes=arxiv_overlap_minutes,
        arxiv_request_timeout_seconds=arxiv_request_timeout_seconds,
        arxiv_request_max_retries=arxiv_request_max_retries,
        arxiv_retry_backoff_seconds=arxiv_retry_backoff_seconds,
        arxiv_user_agent=arxiv_user_agent,
        pghost=os.environ["PGHOST"],
        pgport=parse_positive_int(os.environ["PGPORT"], "PGPORT"),
        pgdatabase=os.environ["PGDATABASE"],
        pguser=os.environ["PGUSER"],
        pgpassword=os.environ["PGPASSWORD"],
    )


def stable_hash(payload: dict[str, Any]) -> str:
    """Create a deterministic SHA256 hash for idempotent row identity checks."""

    normalized = json.dumps(payload, sort_keys=True, ensure_ascii=True, default=str)
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def strip_text(value: str | None) -> str:
    """Trim whitespace and normalize None to an empty string."""

    return (value or "").strip()


def parse_iso_timestamp(value: str | None) -> datetime | None:
    """Parse ISO timestamps from ArXiv feed values."""

    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def normalize_arxiv_id(full_id: str | None) -> tuple[str | None, str | None]:
    """Split full ArXiv id URL into base id and versioned id."""

    if not full_id:
        return None, None

    match = re.search(r"/abs/([^/]+)$", full_id.strip())
    if match:
        versioned = match.group(1)
    else:
        versioned = full_id.strip().rsplit("/", 1)[-1]

    base_id = re.sub(r"v\d+$", "", versioned)
    return base_id or None, versioned or None


def extract_link(
    links: list[dict[str, Any]],
    *,
    rel: str | None = None,
    link_type: str | None = None,
    title: str | None = None,
) -> str | None:
    """Extract the first link matching the requested link attributes."""

    for link in links:
        if rel is not None and link.get("rel") != rel:
            continue
        if link_type is not None and link.get("type") != link_type:
            continue
        if title is not None and link.get("title") != title:
            continue
        href = strip_text(link.get("href"))
        if href:
            return href
    return None


def entry_element_to_record(entry: ET.Element) -> dict[str, Any]:
    """Convert one Atom entry into a normalized Python record."""

    links: list[dict[str, Any]] = []
    for link in entry.findall(f"{{{ARXIV_NS}}}link"):
        links.append({key: value for key, value in link.attrib.items()})

    authors: list[dict[str, Any]] = []
    for order, author in enumerate(entry.findall(f"{{{ARXIV_NS}}}author"), start=1):
        name = strip_text(author.findtext(f"{{{ARXIV_NS}}}name"))
        affiliation = strip_text(author.findtext(f"{{{ARXIV_META_NS}}}affiliation"))
        authors.append(
            {
                "author_order": order,
                "author_name": name,
                "affiliation": affiliation or None,
            }
        )

    categories = [
        strip_text(category.attrib.get("term"))
        for category in entry.findall(f"{{{ARXIV_NS}}}category")
        if strip_text(category.attrib.get("term"))
    ]

    full_id = strip_text(entry.findtext(f"{{{ARXIV_NS}}}id"))
    paper_key, versioned_id = normalize_arxiv_id(full_id)
    title = " ".join(strip_text(entry.findtext(f"{{{ARXIV_NS}}}title")).split())
    summary = " ".join(strip_text(entry.findtext(f"{{{ARXIV_NS}}}summary")).split())
    published_at = parse_iso_timestamp(strip_text(entry.findtext(f"{{{ARXIV_NS}}}published")))
    updated_at = parse_iso_timestamp(strip_text(entry.findtext(f"{{{ARXIV_NS}}}updated")))
    primary_category_node = entry.find(f"{{{ARXIV_META_NS}}}primary_category")
    primary_category = strip_text(primary_category_node.attrib.get("term") if primary_category_node is not None else None)

    raw_payload = {
        "paper_key": paper_key,
        "versioned_id": versioned_id,
        "full_id": full_id,
        "title": title,
        "summary": summary,
        "published_at": published_at.isoformat() if published_at else None,
        "updated_at": updated_at.isoformat() if updated_at else None,
        "primary_category": primary_category or None,
        "categories": categories,
        "authors": authors,
        "links": links,
        "comment": strip_text(entry.findtext(f"{{{ARXIV_META_NS}}}comment")) or None,
        "journal_ref": strip_text(entry.findtext(f"{{{ARXIV_META_NS}}}journal_ref")) or None,
        "doi": strip_text(entry.findtext(f"{{{ARXIV_META_NS}}}doi")) or None,
    }

    return {
        "paper_key": paper_key,
        "versioned_id": versioned_id,
        "title": title,
        "summary": summary,
        "published_at": published_at,
        "updated_at": updated_at,
        "primary_category": primary_category,
        "categories": categories,
        "authors": authors,
        "author_count": len(authors),
        "first_author": authors[0]["author_name"] if authors else None,
        "authors_preview": ", ".join(author["author_name"] for author in authors[:3]),
        "arxiv_url": extract_link(links, rel="alternate", link_type="text/html") or (
            f"https://arxiv.org/abs/{paper_key}" if paper_key else None
        ),
        "pdf_url": extract_link(links, rel="related", link_type="application/pdf", title="pdf"),
        "comment": strip_text(entry.findtext(f"{{{ARXIV_META_NS}}}comment")) or None,
        "journal_ref": strip_text(entry.findtext(f"{{{ARXIV_META_NS}}}journal_ref")) or None,
        "doi": strip_text(entry.findtext(f"{{{ARXIV_META_NS}}}doi")) or None,
        "raw_payload": raw_payload,
        "row_hash": stable_hash(raw_payload),
    }


def build_search_query(base_query: str, start_dt: datetime, end_dt: datetime) -> str:
    """Append a submittedDate range filter to an ArXiv query."""

    start_token = start_dt.strftime("%Y%m%d%H%M")
    end_token = end_dt.strftime("%Y%m%d%H%M")
    return f"({base_query}) AND submittedDate:[{start_token} TO {end_token}]"


def request_feed_page(config: PipelineConfig, params: dict[str, Any]) -> Response:
    """Fetch one feed page with bounded retries and linear backoff."""

    last_error: Exception | None = None
    for attempt in range(1, config.arxiv_request_max_retries + 1):
        try:
            response = requests.get(
                config.arxiv_base_url,
                params=params,
                headers={"User-Agent": config.arxiv_user_agent},
                timeout=config.arxiv_request_timeout_seconds,
            )
            if response.status_code == 429:
                raise requests.HTTPError("429 Too Many Requests", response=response)
            response.raise_for_status()
            return response
        except RequestException as exc:
            last_error = exc
            if attempt == config.arxiv_request_max_retries:
                break

            is_rate_limited = getattr(getattr(exc, "response", None), "status_code", None) == 429
            if is_rate_limited:
                sleep_seconds = max(30, config.arxiv_retry_backoff_seconds * attempt * 6)
            else:
                sleep_seconds = config.arxiv_retry_backoff_seconds * attempt
            LOGGER.warning(
                "ArXiv request failed; retrying",
                extra={
                    "context": {
                        "attempt": attempt,
                        "max_retries": config.arxiv_request_max_retries,
                        "rate_limited": is_rate_limited,
                        "sleep_seconds": sleep_seconds,
                        "error_type": type(exc).__name__,
                        "error": str(exc),
                    }
                },
            )
            time.sleep(sleep_seconds)

    raise RuntimeError("ArXiv request retries exhausted") from last_error


def fetch_feed_page(
    config: PipelineConfig,
    search_query: str,
    start: int,
    max_results: int,
) -> tuple[int, list[dict[str, Any]]]:
    """Fetch one page of ArXiv feed results and parse records."""

    params = {
        "search_query": search_query,
        "start": start,
        "max_results": max_results,
        "sortBy": "submittedDate",
        "sortOrder": "ascending",
    }
    response = request_feed_page(config, params)

    root = ET.fromstring(response.text)
    total_results_text = root.findtext(f"{{{OPENSEARCH_NS}}}totalResults", default="0")
    total_results = int(total_results_text)

    entries = [entry_element_to_record(entry) for entry in root.findall(f"{{{ARXIV_NS}}}entry")]
    return total_results, entries


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
        paper_key TEXT UNIQUE,
        batch_id TEXT NOT NULL,
        page_number INTEGER NOT NULL,
        row_hash TEXT NOT NULL UNIQUE,
        reason TEXT NOT NULL,
        raw_payload JSONB NOT NULL,
        rejected_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
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
        connection.execute(text(create_state_sql))


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


def upsert_state(engine: Engine, source_name: str, watermark_published_at: datetime, watermark_paper_key: str, batch_id: str) -> None:
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


def split_clean_and_reject(entries: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Apply defensive validation and split entries into clean and rejected lists."""

    clean_entries: list[dict[str, Any]] = []
    rejected_entries: list[dict[str, Any]] = []

    for entry in entries:
        reasons: list[str] = []

        if not entry.get("paper_key"):
            reasons.append("missing_paper_key")
        if not entry.get("title"):
            reasons.append("missing_title")
        if not entry.get("summary"):
            reasons.append("missing_summary")
        if entry.get("published_at") is None:
            reasons.append("missing_or_invalid_published_at")
        if entry.get("updated_at") is None:
            reasons.append("missing_or_invalid_updated_at")
        if not entry.get("primary_category"):
            reasons.append("missing_primary_category")

        valid_authors = [author for author in entry.get("authors", []) if strip_text(author.get("author_name"))]
        if not valid_authors:
            reasons.append("authors_without_names")

        if reasons:
            rejected_entries.append(
                {
                    "paper_key": entry.get("paper_key"),
                    "row_hash": entry.get("row_hash") or stable_hash(entry.get("raw_payload", {})),
                    "reason": ";".join(reasons),
                    "raw_payload": entry.get("raw_payload", {}),
                    "entry": entry,
                }
            )
            continue

        clean_entries.append(entry)

    return clean_entries, rejected_entries


def upsert_raw_records(engine: Engine, batch_id: str, source_query: str, source_start_at: datetime, page_number: int, entries: list[dict[str, Any]]) -> None:
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
            author_name = strip_text(author.get("author_name"))
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


def upsert_rejected_records(engine: Engine, batch_id: str, page_number: int, rejected_entries: list[dict[str, Any]]) -> None:
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
        ON CONFLICT (paper_key)
        DO UPDATE SET
            batch_id = EXCLUDED.batch_id,
            page_number = EXCLUDED.page_number,
            row_hash = EXCLUDED.row_hash,
            reason = EXCLUDED.reason,
            raw_payload = EXCLUDED.raw_payload,
            rejected_at = NOW();
        """
    )

    with engine.begin() as connection:
        connection.execute(sql, records)


def export_rejected_csv_sample(rejected_entries: list[dict[str, Any]], batch_id: str) -> Path:
    """Export a small CSV sample of rejected rows for classroom inspection."""

    output_dir = Path(__file__).resolve().parent / "output"
    output_dir.mkdir(parents=True, exist_ok=True)

    output_path = output_dir / f"rejected_sample_{batch_id}.csv"
    sample_rows = []
    for entry in rejected_entries[:100]:
        sample_rows.append(
            {
                "paper_key": entry.get("paper_key"),
                "row_hash": entry.get("row_hash"),
                "reason": entry.get("reason"),
                "raw_payload": json.dumps(entry.get("raw_payload", {}), ensure_ascii=True, default=str),
            }
        )

    if sample_rows:
        with output_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(sample_rows[0].keys()))
            writer.writeheader()
            writer.writerows(sample_rows)

    return output_path


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


def run_pipeline() -> None:
    """Execute the end-to-end Day 2 ArXiv ingestion pipeline."""

    config = load_config()
    batch_id = str(uuid4())
    engine = create_engine(config.sqlalchemy_url, future=True)

    ensure_schema_and_tables(engine)

    current_state = get_state(engine, config.arxiv_source_name)
    now = datetime.now(timezone.utc)
    if current_state:
        source_start_at = current_state["watermark_published_at"] - timedelta(minutes=config.arxiv_overlap_minutes)
    else:
        source_start_at = now - timedelta(days=config.arxiv_default_lookback_days)

    source_end_at = now
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

    if latest_seen_entry and latest_seen_entry.get("published_at") is not None:
        upsert_state(
            engine,
            config.arxiv_source_name,
            latest_seen_entry["published_at"],
            latest_seen_entry["paper_key"],
            batch_id,
        )

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


if __name__ == "__main__":
    try:
        run_pipeline()
    except Exception as exc:  # pragma: no cover
        LOGGER.exception(
            "Pipeline failed",
            extra={"context": {"error_type": type(exc).__name__, "error": str(exc)}},
        )
        raise