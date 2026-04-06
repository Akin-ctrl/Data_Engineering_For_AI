"""Day 1 pipeline: download a raw HuggingFace CSV, clean defensively, and load into PostgreSQL.

This script is intentionally written as a one-shot executable for foundational teaching.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import unquote
from uuid import uuid4

import pandas as pd
import requests
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine


SCHEMA_NAME = "de_ai"
RAW_TABLE = "raw_reviews"
CLEAN_TABLE = "clean_reviews"
REJECT_TABLE = "rejected_reviews"

EXPECTED_COLUMNS = [
    "unnamed_0",
    "clothing_id",
    "age",
    "title",
    "review_text",
    "rating",
    "recommended_ind",
    "positive_feedback_count",
    "division_name",
    "department_name",
    "class_name",
]


class JsonFormatter(logging.Formatter):
    """Simple JSON formatter for structured logging."""

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
    """Create and return a JSON logger."""
    logger = logging.getLogger("day1_pipeline")
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(JsonFormatter())
        logger.addHandler(handler)

    return logger


LOGGER = configure_logger()


@dataclass(frozen=True)
class PipelineConfig:
    """Configuration required to run the Day 1 pipeline."""

    hf_csv_url: str
    pghost: str
    pgport: int
    pgdatabase: str
    pguser: str
    pgpassword: str

    @property
    def sqlalchemy_url(self) -> str:
        """Build SQLAlchemy connection URL for PostgreSQL."""
        return (
            f"postgresql+psycopg://{self.pguser}:{self.pgpassword}"
            f"@{self.pghost}:{self.pgport}/{self.pgdatabase}"
        )


def load_config() -> PipelineConfig:
    """Load and validate environment configuration from .env."""
    load_dotenv()

    required = [
        "HF_CSV_URL",
        "PGHOST",
        "PGPORT",
        "PGDATABASE",
        "PGUSER",
        "PGPASSWORD",
    ]

    missing = [key for key in required if not os.getenv(key)]
    if missing:
        raise ValueError(f"Missing required environment variables: {', '.join(missing)}")

    return PipelineConfig(
        hf_csv_url=os.environ["HF_CSV_URL"],
        pghost=os.environ["PGHOST"],
        pgport=int(os.environ["PGPORT"]),
        pgdatabase=os.environ["PGDATABASE"],
        pguser=os.environ["PGUSER"],
        pgpassword=os.environ["PGPASSWORD"],
    )


def normalize_column_name(column_name: str) -> str:
    """Normalize a column name to safe snake_case."""
    lowered = column_name.strip().lower()
    normalized = re.sub(r"[^a-z0-9]+", "_", lowered)
    normalized = re.sub(r"_+", "_", normalized).strip("_")
    return normalized


def stable_row_hash(row: pd.Series) -> str:
    """Create a deterministic hash for a row after string normalization."""
    normalized = {
        str(key): "" if pd.isna(value) else str(value).strip()
        for key, value in row.to_dict().items()
    }
    row_json = json.dumps(normalized, sort_keys=True, ensure_ascii=True)
    return hashlib.sha256(row_json.encode("utf-8")).hexdigest()


def find_first_existing_column(df: pd.DataFrame, candidates: list[str]) -> str | None:
    """Return the first candidate column that exists in dataframe columns."""
    existing = set(df.columns)
    for name in candidates:
        if name in existing:
            return name
    return None


def parse_hf_dataset_id(url: str) -> str | None:
    """Extract dataset identifier owner/name from a Hugging Face dataset URL."""
    match = re.search(r"huggingface\.co/datasets/([^/]+/[^/?#]+)", url)
    if not match:
        return None
    return unquote(match.group(1))


def resolve_huggingface_csv_url(input_url: str) -> str:
    """Resolve a Hugging Face dataset page URL into a direct CSV download URL."""
    if "/resolve/" in input_url and input_url.lower().endswith(".csv"):
        return input_url

    dataset_id = parse_hf_dataset_id(input_url)
    if not dataset_id:
        raise ValueError("HF_CSV_URL is not a valid Hugging Face dataset URL.")

    api_url = f"https://huggingface.co/api/datasets/{dataset_id}"
    response = requests.get(api_url, timeout=30)
    response.raise_for_status()
    metadata = response.json()

    siblings: Iterable[dict[str, Any]] = metadata.get("siblings", [])
    csv_file = next((s.get("rfilename") for s in siblings if str(s.get("rfilename", "")).lower().endswith(".csv")), None)
    if not csv_file:
        raise ValueError(f"No CSV file found in dataset repository: {dataset_id}")

    encoded_filename = csv_file.replace(" ", "%20")
    return f"https://huggingface.co/datasets/{dataset_id}/resolve/main/{encoded_filename}"


def download_csv(url: str, destination: Path) -> Path:
    """Download a CSV file from URL to destination path."""
    destination.parent.mkdir(parents=True, exist_ok=True)
    response = requests.get(url, timeout=60)
    response.raise_for_status()
    destination.write_bytes(response.content)
    return destination


def read_csv_defensively(csv_path: Path) -> pd.DataFrame:
    """Read CSV with resilient options suited for beginner pipelines."""
    return pd.read_csv(
        csv_path,
        dtype=str,
        keep_default_na=True,
        na_values=["", "null", "NULL", "None", "none", "NaN", "nan"],
        on_bad_lines="skip",
        encoding="utf-8",
    )


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


def clean_dataset_defensively(df_raw: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Apply defensive cleaning and split records into clean and rejected sets."""
    df = df_raw.copy()
    df.columns = [normalize_column_name(col) for col in df.columns]

    df = df.reset_index(drop=True)
    df["row_number"] = df.index + 1
    df["row_hash"] = df.apply(stable_row_hash, axis=1)

    missing_columns = [col for col in EXPECTED_COLUMNS if col not in df.columns]
    if missing_columns:
        raise ValueError(
            "Dataset does not match expected schema. Missing columns: "
            + ", ".join(missing_columns)
        )

    df["review_id"] = pd.to_numeric(df["unnamed_0"], errors="coerce")
    df["clothing_id"] = pd.to_numeric(df["clothing_id"], errors="coerce")
    df["age"] = pd.to_numeric(df["age"], errors="coerce")
    df["rating"] = pd.to_numeric(df["rating"], errors="coerce")
    df["recommended_ind"] = pd.to_numeric(df["recommended_ind"], errors="coerce")
    df["positive_feedback_count"] = pd.to_numeric(df["positive_feedback_count"], errors="coerce")

    for text_col in [
        "title",
        "review_text",
        "division_name",
        "department_name",
        "class_name",
    ]:
        df[text_col] = df[text_col].astype(str).str.strip()

    df["review_key"] = (
        df["review_id"].astype("Int64").astype(str).where(df["review_id"].notna(), df["row_hash"])
    )

    reasons: list[list[str]] = []
    for _, row in df.iterrows():
        row_reasons: list[str] = []

        if pd.isna(row["clothing_id"]):
            row_reasons.append("missing_or_invalid_clothing_id")
        if pd.isna(row["rating"]):
            row_reasons.append("missing_or_invalid_rating")
        elif int(row["rating"]) < 1 or int(row["rating"]) > 5:
            row_reasons.append("rating_out_of_range_1_to_5")
        if pd.isna(row["recommended_ind"]):
            row_reasons.append("missing_or_invalid_recommended_ind")
        elif int(row["recommended_ind"]) not in {0, 1}:
            row_reasons.append("recommended_ind_not_binary")
        if pd.isna(row["positive_feedback_count"]):
            row_reasons.append("missing_or_invalid_positive_feedback_count")
        elif int(row["positive_feedback_count"]) < 0:
            row_reasons.append("negative_positive_feedback_count")

        review_text = str(row["review_text"]).strip().lower()
        if not review_text or review_text in {"nan", "none"}:
            row_reasons.append("missing_review_text")

        if not pd.isna(row["age"]):
            age_value = int(row["age"])
            if age_value < 13 or age_value > 100:
                row_reasons.append("age_out_of_expected_range")

        reasons.append(row_reasons)

    df["reject_reason"] = [";".join(x) for x in reasons]
    is_rejected = df["reject_reason"] != ""

    base_payload_columns = [col for col in df_raw.columns]
    raw_payload = df_raw.copy()
    raw_payload.columns = [normalize_column_name(col) for col in base_payload_columns]
    raw_payload = raw_payload.where(pd.notna(raw_payload), None)
    df["raw_payload"] = raw_payload.to_dict(orient="records")

    df_clean = df.loc[~is_rejected].copy()
    df_reject = df.loc[is_rejected].copy()

    clean_columns = [
        "review_key",
        "row_hash",
        "row_number",
        "review_id",
        "clothing_id",
        "age",
        "title",
        "review_text",
        "rating",
        "recommended_ind",
        "positive_feedback_count",
        "division_name",
        "department_name",
        "class_name",
        "raw_payload",
    ]
    reject_columns = ["row_hash", "row_number", "reject_reason", "raw_payload"]

    return df_clean[clean_columns], df_reject[reject_columns]


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


def export_rejected_csv_sample(df_reject: pd.DataFrame, batch_id: str) -> Path:
    """Persist a small rejected sample for teaching and troubleshooting."""
    output_dir = Path(__file__).resolve().parent / "output"
    output_dir.mkdir(parents=True, exist_ok=True)

    output_path = output_dir / f"rejected_sample_{batch_id}.csv"
    sample_df = df_reject.copy()
    sample_df["raw_payload"] = sample_df["raw_payload"].apply(
        lambda payload: json.dumps(payload, ensure_ascii=True)
    )
    sample_df.head(100).to_csv(output_path, index=False)
    return output_path


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


def run_pipeline() -> None:
    """Execute the full Day 1 one-shot ingestion pipeline."""
    config = load_config()
    batch_id = str(uuid4())

    root_dir = Path(__file__).resolve().parents[1]
    raw_dir = root_dir / "day1" / "output"
    raw_path = raw_dir / f"raw_download_{batch_id}.csv"

    LOGGER.info(
        "Pipeline started",
        extra={"context": {"batch_id": batch_id, "source_url": config.hf_csv_url}},
    )

    resolved_source_url = resolve_huggingface_csv_url(config.hf_csv_url)
    downloaded_path = download_csv(resolved_source_url, raw_path)
    LOGGER.info(
        "CSV downloaded",
        extra={
            "context": {
                "batch_id": batch_id,
                "csv_path": str(downloaded_path),
                "resolved_source_url": resolved_source_url,
            }
        },
    )

    df_raw = read_csv_defensively(downloaded_path)
    if df_raw.empty:
        raise ValueError("Downloaded CSV is empty. Stopping pipeline.")

    engine = create_engine(config.sqlalchemy_url, future=True)
    ensure_schema_and_tables(engine)

    # Preserve a normalized copy for raw table ingestion and defensive traceability.
    df_raw_normalized = df_raw.copy()
    df_raw_normalized.columns = [normalize_column_name(col) for col in df_raw_normalized.columns]
    df_raw_normalized = df_raw_normalized.reset_index(drop=True)
    df_raw_normalized["row_number"] = df_raw_normalized.index + 1
    df_raw_normalized["row_hash"] = df_raw_normalized.apply(stable_row_hash, axis=1)

    upsert_raw_records(engine, batch_id, resolved_source_url, df_raw_normalized)
    df_clean, df_reject = clean_dataset_defensively(df_raw)

    upsert_clean_records(engine, batch_id, df_clean)
    upsert_rejected_records(engine, batch_id, df_reject)

    rejected_sample_path = export_rejected_csv_sample(df_reject, batch_id)
    run_post_load_checks(engine, batch_id)

    LOGGER.info(
        "Pipeline finished successfully",
        extra={
            "context": {
                "batch_id": batch_id,
                "raw_rows": int(len(df_raw)),
                "clean_rows": int(len(df_clean)),
                "rejected_rows": int(len(df_reject)),
                "rejected_sample_csv": str(rejected_sample_path),
            }
        },
    )


if __name__ == "__main__":
    try:
        run_pipeline()
    except Exception as exc:  # pragma: no cover - explicit entrypoint guard
        LOGGER.exception(
            "Pipeline failed",
            extra={"context": {"error_type": type(exc).__name__, "error": str(exc)}},
        )
        raise
