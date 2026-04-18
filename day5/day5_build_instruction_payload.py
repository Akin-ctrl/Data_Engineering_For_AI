"""Day 5 pipeline: chunk clean paper text and export strict instruction-tuning payloads.

This script reads Day 2 clean paper records from PostgreSQL, chunks text with a
sentence-based strategy and overlap, formats deterministic instruction pairs,
validates payload quality, and writes dual exports for fine-tuning.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import random
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean
from typing import Any

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text


DEFAULT_SOURCE_TABLE = "training_data.clean_papers"
DEFAULT_ORDER_BY = "paper_key"
DEFAULT_OUTPUT_DIR = "day5/output"
DEFAULT_EXPORT_BASENAME = "day5_instruction_payload"
DEFAULT_MAX_CHUNK_WORDS = 350
DEFAULT_CHUNK_OVERLAP_WORDS = 50
DEFAULT_MIN_INPUT_WORDS = 30
DEFAULT_MIN_OUTPUT_WORDS = 3
DEFAULT_TRAIN_RATIO = 0.9
DEFAULT_SPLIT_SEED = 42
DEFAULT_MAX_PAPERS = 0
IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)*$")
SIMPLE_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class JsonFormatter(logging.Formatter):
    """Format logs as machine-readable JSON."""

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
    """Create the Day 5 logger once with JSON output."""

    logger = logging.getLogger("day5_payload")
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(JsonFormatter())
        logger.addHandler(handler)

    return logger


LOGGER = configure_logger()


@dataclass(frozen=True)
class Day5Config:
    """Typed configuration for Day 5 instruction payload generation."""

    pghost: str
    pgport: int
    pgdatabase: str
    pguser: str
    pgpassword: str
    source_table: str
    order_by: str
    output_dir: Path
    export_basename: str
    max_chunk_words: int
    chunk_overlap_words: int
    min_input_words: int
    min_output_words: int
    train_ratio: float
    split_seed: int
    max_papers: int

    @property
    def sqlalchemy_url(self) -> str:
        """Build SQLAlchemy URL for PostgreSQL access."""

        return (
            f"postgresql+psycopg://{self.pguser}:{self.pgpassword}"
            f"@{self.pghost}:{self.pgport}/{self.pgdatabase}"
        )

    @property
    def alpaca_all_path(self) -> Path:
        """Full Alpaca-style payload path."""

        return self.output_dir / f"{self.export_basename}_alpaca_all.jsonl"

    @property
    def alpaca_train_path(self) -> Path:
        """Train split Alpaca-style payload path."""

        return self.output_dir / f"{self.export_basename}_alpaca_train.jsonl"

    @property
    def alpaca_val_path(self) -> Path:
        """Validation split Alpaca-style payload path."""

        return self.output_dir / f"{self.export_basename}_alpaca_val.jsonl"

    @property
    def chat_all_path(self) -> Path:
        """Full chat-style payload path."""

        return self.output_dir / f"{self.export_basename}_chat_all.jsonl"

    @property
    def chat_train_path(self) -> Path:
        """Train split chat-style payload path."""

        return self.output_dir / f"{self.export_basename}_chat_train.jsonl"

    @property
    def chat_val_path(self) -> Path:
        """Validation split chat-style payload path."""

        return self.output_dir / f"{self.export_basename}_chat_val.jsonl"

    @property
    def manifest_path(self) -> Path:
        """Manifest path with run summary and data quality metrics."""

        return self.output_dir / f"{self.export_basename}_manifest.json"


@dataclass(frozen=True)
class ChunkRecord:
    """A sentence-based chunk built from one source paper."""

    chunk_id: str
    paper_key: str
    primary_category: str
    title: str
    chunk_index: int
    chunk_text: str


@dataclass(frozen=True)
class AlpacaRecord:
    """Strict Alpaca-style instruction record."""

    id: str
    task_type: str
    paper_key: str
    chunk_id: str
    instruction: str
    input: str
    output: str
    metadata: dict[str, Any]


@dataclass(frozen=True)
class ChatRecord:
    """Strict chat-style instruction record."""

    id: str
    task_type: str
    paper_key: str
    chunk_id: str
    messages: list[dict[str, str]]
    metadata: dict[str, Any]


def validate_identifier(value: str, *, pattern: re.Pattern[str], field_name: str) -> str:
    """Validate SQL identifier-like values before SQL interpolation."""

    cleaned_value = value.strip()
    if not cleaned_value:
        raise ValueError(f"{field_name} must not be empty")
    if not pattern.match(cleaned_value):
        raise ValueError(f"{field_name} has an invalid format: {cleaned_value}")
    return cleaned_value


def parse_positive_int(value: str | None, field_name: str, default: int) -> int:
    """Parse a positive integer with a safe fallback default."""

    if value is None or not str(value).strip():
        return default

    try:
        parsed_value = int(value)
    except ValueError as exc:  # pragma: no cover - defensive branch
        raise ValueError(f"{field_name} must be an integer, got: {value}") from exc

    if parsed_value <= 0:
        raise ValueError(f"{field_name} must be greater than zero, got: {parsed_value}")

    return parsed_value


def parse_non_negative_int(value: str | None, field_name: str, default: int) -> int:
    """Parse a non-negative integer with a safe fallback default."""

    if value is None or not str(value).strip():
        return default

    try:
        parsed_value = int(value)
    except ValueError as exc:  # pragma: no cover - defensive branch
        raise ValueError(f"{field_name} must be an integer, got: {value}") from exc

    if parsed_value < 0:
        raise ValueError(f"{field_name} must be zero or greater, got: {parsed_value}")

    return parsed_value


def parse_ratio(value: str | None, field_name: str, default: float) -> float:
    """Parse a split ratio in the open interval (0, 1)."""

    if value is None or not str(value).strip():
        return default

    try:
        parsed_value = float(value)
    except ValueError as exc:  # pragma: no cover - defensive branch
        raise ValueError(f"{field_name} must be a float, got: {value}") from exc

    if parsed_value <= 0 or parsed_value >= 1:
        raise ValueError(f"{field_name} must be between 0 and 1, got: {parsed_value}")

    return parsed_value


def resolve_relative_path(path_value: str, *, base_dir: Path) -> Path:
    """Resolve environment path values against repository root."""

    path = Path(path_value).expanduser()
    if path.is_absolute():
        return path
    return base_dir / path


def load_config() -> Day5Config:
    """Load and validate Day 5 configuration from environment variables."""

    load_dotenv()

    required = ["PGHOST", "PGPORT", "PGDATABASE", "PGUSER", "PGPASSWORD"]
    missing = [name for name in required if not os.getenv(name)]
    if missing:
        raise ValueError(f"Missing required environment variables: {', '.join(missing)}")

    repo_root = Path(__file__).resolve().parents[1]
    source_table = validate_identifier(
        os.getenv("DAY5_SOURCE_TABLE", DEFAULT_SOURCE_TABLE),
        pattern=IDENTIFIER_PATTERN,
        field_name="DAY5_SOURCE_TABLE",
    )
    order_by = validate_identifier(
        os.getenv("DAY5_ORDER_BY", DEFAULT_ORDER_BY),
        pattern=SIMPLE_IDENTIFIER_PATTERN,
        field_name="DAY5_ORDER_BY",
    )
    output_dir = resolve_relative_path(
        os.getenv("DAY5_OUTPUT_DIR", DEFAULT_OUTPUT_DIR),
        base_dir=repo_root,
    )
    export_basename = validate_identifier(
        os.getenv("DAY5_EXPORT_BASENAME", DEFAULT_EXPORT_BASENAME),
        pattern=re.compile(r"^[A-Za-z0-9_-]+$"),
        field_name="DAY5_EXPORT_BASENAME",
    )

    max_chunk_words = parse_positive_int(
        os.getenv("DAY5_MAX_CHUNK_WORDS"),
        "DAY5_MAX_CHUNK_WORDS",
        DEFAULT_MAX_CHUNK_WORDS,
    )
    chunk_overlap_words = parse_non_negative_int(
        os.getenv("DAY5_CHUNK_OVERLAP_WORDS"),
        "DAY5_CHUNK_OVERLAP_WORDS",
        DEFAULT_CHUNK_OVERLAP_WORDS,
    )
    if chunk_overlap_words >= max_chunk_words:
        raise ValueError("DAY5_CHUNK_OVERLAP_WORDS must be smaller than DAY5_MAX_CHUNK_WORDS")

    min_input_words = parse_positive_int(
        os.getenv("DAY5_MIN_INPUT_WORDS"),
        "DAY5_MIN_INPUT_WORDS",
        DEFAULT_MIN_INPUT_WORDS,
    )
    min_output_words = parse_positive_int(
        os.getenv("DAY5_MIN_OUTPUT_WORDS"),
        "DAY5_MIN_OUTPUT_WORDS",
        DEFAULT_MIN_OUTPUT_WORDS,
    )
    train_ratio = parse_ratio(
        os.getenv("DAY5_TRAIN_RATIO"),
        "DAY5_TRAIN_RATIO",
        DEFAULT_TRAIN_RATIO,
    )
    split_seed = parse_positive_int(
        os.getenv("DAY5_SPLIT_SEED"),
        "DAY5_SPLIT_SEED",
        DEFAULT_SPLIT_SEED,
    )
    max_papers = parse_non_negative_int(
        os.getenv("DAY5_MAX_PAPERS"),
        "DAY5_MAX_PAPERS",
        DEFAULT_MAX_PAPERS,
    )

    return Day5Config(
        pghost=os.environ["PGHOST"],
        pgport=parse_positive_int(os.environ["PGPORT"], "PGPORT", 5432),
        pgdatabase=os.environ["PGDATABASE"],
        pguser=os.environ["PGUSER"],
        pgpassword=os.environ["PGPASSWORD"],
        source_table=source_table,
        order_by=order_by,
        output_dir=output_dir,
        export_basename=export_basename,
        max_chunk_words=max_chunk_words,
        chunk_overlap_words=chunk_overlap_words,
        min_input_words=min_input_words,
        min_output_words=min_output_words,
        train_ratio=train_ratio,
        split_seed=split_seed,
        max_papers=max_papers,
    )


def normalize_whitespace(value: str) -> str:
    """Collapse internal whitespace and trim edges."""

    return re.sub(r"\s+", " ", value.strip())


def split_sentences(text_value: str) -> list[str]:
    """Split text into sentence-like units for chunk assembly."""

    cleaned = normalize_whitespace(text_value)
    if not cleaned:
        return []

    sentences = re.split(r"(?<=[.!?])\s+", cleaned)
    return [sentence.strip() for sentence in sentences if sentence.strip()]


def word_count(text_value: str) -> int:
    """Count words using simple whitespace tokenization."""

    return len(text_value.split())


def tail_words(text_value: str, count: int) -> str:
    """Return the last N words from text for overlap bridging."""

    if count <= 0:
        return ""
    words = text_value.split()
    if not words:
        return ""
    return " ".join(words[-count:])


def split_long_sentence(sentence: str, max_words: int, overlap_words: int) -> list[str]:
    """Split an oversized sentence into overlap-preserving windows."""

    words = sentence.split()
    if len(words) <= max_words:
        return [sentence]

    chunks: list[str] = []
    step = max(1, max_words - overlap_words)
    start = 0
    while start < len(words):
        chunk_words = words[start : start + max_words]
        chunks.append(" ".join(chunk_words))
        if start + max_words >= len(words):
            break
        start += step
    return chunks


def chunk_text(
    text_value: str,
    *,
    max_words: int,
    overlap_words: int,
) -> list[str]:
    """Build sentence-first chunks with optional word overlap."""

    sentences = split_sentences(text_value)
    if not sentences:
        return []

    expanded_sentences: list[str] = []
    for sentence in sentences:
        expanded_sentences.extend(split_long_sentence(sentence, max_words, overlap_words))

    chunks: list[str] = []
    current_sentences: list[str] = []
    current_words = 0

    for sentence in expanded_sentences:
        sentence_words = word_count(sentence)

        if current_sentences and (current_words + sentence_words) > max_words:
            finalized = normalize_whitespace(" ".join(current_sentences))
            chunks.append(finalized)
            overlap_prefix = tail_words(finalized, overlap_words)
            if overlap_prefix:
                current_sentences = [overlap_prefix, sentence]
                current_words = word_count(overlap_prefix) + sentence_words
            else:
                current_sentences = [sentence]
                current_words = sentence_words
            continue

        current_sentences.append(sentence)
        current_words += sentence_words

    if current_sentences:
        chunks.append(normalize_whitespace(" ".join(current_sentences)))

    return [chunk for chunk in chunks if chunk]


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


def build_chunk_records(config: Day5Config, frame: pd.DataFrame) -> list[ChunkRecord]:
    """Convert source papers into sentence-overlap chunk records."""

    chunk_records: list[ChunkRecord] = []

    for _, row in frame.iterrows():
        paper_key = normalize_whitespace(str(row.get("paper_key", "")))
        title = normalize_whitespace(str(row.get("title", "")))
        summary = normalize_whitespace(str(row.get("summary", "")))
        primary_category = normalize_whitespace(str(row.get("primary_category", "")))

        if not paper_key or not summary or not primary_category:
            continue

        source_text = f"Title: {title}. Summary: {summary}" if title else f"Summary: {summary}"
        chunks = chunk_text(
            source_text,
            max_words=config.max_chunk_words,
            overlap_words=config.chunk_overlap_words,
        )

        for chunk_index, chunk in enumerate(chunks, start=1):
            if word_count(chunk) < config.min_input_words:
                continue
            digest_input = f"{paper_key}|{chunk_index}|{chunk}".encode("utf-8")
            chunk_id = hashlib.sha256(digest_input).hexdigest()[:16]
            chunk_records.append(
                ChunkRecord(
                    chunk_id=chunk_id,
                    paper_key=paper_key,
                    primary_category=primary_category,
                    title=title,
                    chunk_index=chunk_index,
                    chunk_text=chunk,
                )
            )

    if not chunk_records:
        raise ValueError("No valid chunk records were generated from source data")

    return chunk_records


def extractive_summary(chunk_text_value: str, max_sentences: int = 3) -> str:
    """Build a concise extractive summary from leading sentences."""

    sentences = split_sentences(chunk_text_value)
    if not sentences:
        return chunk_text_value
    return " ".join(sentences[:max_sentences]).strip()


def keypoints_output(chunk_text_value: str, max_points: int = 5) -> str:
    """Build deterministic key points using leading sentence snippets."""

    sentences = split_sentences(chunk_text_value)
    if not sentences:
        return "- Key point unavailable due to empty chunk."

    points = []
    for sentence in sentences[:max_points]:
        points.append(f"- {sentence}")
    return "\n".join(points)


def build_instruction_triples(chunk: ChunkRecord) -> list[tuple[str, str, str]]:
    """Build deterministic (task_type, instruction, output) tuples per chunk."""

    summary_instruction = (
        "Summarize the research text below in 3-4 concise sentences. "
        "Keep factual details grounded in the source."
    )
    classify_instruction = (
        "Classify the research text below into one ArXiv-style primary category. "
        "Return only the category code."
    )
    keypoints_instruction = (
        "Extract 5 key points from the research text below. "
        "Return one bullet per point."
    )

    return [
        ("summarize", summary_instruction, extractive_summary(chunk.chunk_text, max_sentences=4)),
        ("classify", classify_instruction, chunk.primary_category),
        ("keypoints", keypoints_instruction, keypoints_output(chunk.chunk_text, max_points=5)),
    ]


def build_alpaca_records(chunk_records: list[ChunkRecord]) -> list[AlpacaRecord]:
    """Build strict Alpaca-style records from chunk records."""

    records: list[AlpacaRecord] = []

    for chunk in chunk_records:
        triples = build_instruction_triples(chunk)
        for task_type, instruction, output in triples:
            record_id = f"{chunk.chunk_id}_{task_type}"
            records.append(
                AlpacaRecord(
                    id=record_id,
                    task_type=task_type,
                    paper_key=chunk.paper_key,
                    chunk_id=chunk.chunk_id,
                    instruction=instruction,
                    input=chunk.chunk_text,
                    output=output,
                    metadata={
                        "paper_key": chunk.paper_key,
                        "chunk_index": chunk.chunk_index,
                        "task_type": task_type,
                        "primary_category": chunk.primary_category,
                        "title": chunk.title,
                    },
                )
            )

    return records


def build_chat_records(alpaca_records: list[AlpacaRecord]) -> list[ChatRecord]:
    """Build strict chat-style records from Alpaca-style records."""

    chat_records: list[ChatRecord] = []

    for record in alpaca_records:
        user_message = f"Instruction: {record.instruction}\n\nInput:\n{record.input}"
        chat_records.append(
            ChatRecord(
                id=record.id,
                task_type=record.task_type,
                paper_key=record.paper_key,
                chunk_id=record.chunk_id,
                messages=[
                    {"role": "user", "content": user_message},
                    {"role": "assistant", "content": record.output},
                ],
                metadata=record.metadata,
            )
        )

    return chat_records


def validate_alpaca_records(records: list[AlpacaRecord], config: Day5Config) -> None:
    """Apply strict schema and length checks to Alpaca-style records."""

    if not records:
        raise ValueError("No Alpaca records available for validation")

    seen_ids: set[str] = set()
    output_lengths: list[int] = []

    for index, record in enumerate(records, start=1):
        if record.id in seen_ids:
            raise ValueError(f"Duplicate Alpaca record id detected: {record.id}")
        seen_ids.add(record.id)

        if not record.instruction.strip():
            raise ValueError(f"Empty instruction at Alpaca record index {index}")
        if not record.input.strip():
            raise ValueError(f"Empty input at Alpaca record index {index}")
        if not record.output.strip():
            raise ValueError(f"Empty output at Alpaca record index {index}")

        if word_count(record.input) < config.min_input_words:
            raise ValueError(
                f"Input too short at Alpaca record index {index}: "
                f"{word_count(record.input)} words"
            )
        output_min_words = 1 if record.task_type == "classify" else config.min_output_words
        if word_count(record.output) < output_min_words:
            raise ValueError(
                f"Output too short at Alpaca record index {index}: "
                f"{word_count(record.output)} words"
            )

        output_lengths.append(word_count(record.output))

    LOGGER.info(
        "Alpaca validation passed",
        extra={
            "context": {
                "record_count": len(records),
                "avg_output_words": round(mean(output_lengths), 2),
            }
        },
    )


def validate_chat_records(records: list[ChatRecord]) -> None:
    """Apply strict schema checks to chat-style records."""

    if not records:
        raise ValueError("No chat records available for validation")

    seen_ids: set[str] = set()
    for index, record in enumerate(records, start=1):
        if record.id in seen_ids:
            raise ValueError(f"Duplicate chat record id detected: {record.id}")
        seen_ids.add(record.id)

        if len(record.messages) != 2:
            raise ValueError(f"Invalid message count at chat record index {index}")

        user_message = record.messages[0]
        assistant_message = record.messages[1]

        if user_message.get("role") != "user":
            raise ValueError(f"First role must be user at chat record index {index}")
        if assistant_message.get("role") != "assistant":
            raise ValueError(f"Second role must be assistant at chat record index {index}")
        if not user_message.get("content", "").strip():
            raise ValueError(f"Empty user message content at chat record index {index}")
        if not assistant_message.get("content", "").strip():
            raise ValueError(f"Empty assistant message content at chat record index {index}")


def split_ids(ids: list[str], train_ratio: float, split_seed: int) -> tuple[set[str], set[str]]:
    """Split ids deterministically into train and validation sets."""

    shuffled = ids.copy()
    random.Random(split_seed).shuffle(shuffled)
    split_index = int(len(shuffled) * train_ratio)

    train_ids = set(shuffled[:split_index])
    val_ids = set(shuffled[split_index:])

    if not train_ids or not val_ids:
        raise ValueError(
            "Train/val split produced an empty subset. "
            "Increase data size or adjust DAY5_TRAIN_RATIO."
        )

    return train_ids, val_ids


def write_jsonl(path: Path, records: list[dict[str, Any]]) -> None:
    """Write newline-delimited JSON records deterministically."""

    with path.open("w", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record, ensure_ascii=True, default=str))
            handle.write("\n")


def build_manifest(
    config: Day5Config,
    source_rows: int,
    chunk_records: list[ChunkRecord],
    alpaca_records: list[AlpacaRecord],
    train_ids: set[str],
    val_ids: set[str],
) -> dict[str, Any]:
    """Build run metadata manifest for Day 5 outputs."""

    task_counts: dict[str, int] = {}
    for record in alpaca_records:
        task_counts[record.task_type] = task_counts.get(record.task_type, 0) + 1

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source": {
            "table": config.source_table,
            "rows": source_rows,
            "max_papers": config.max_papers,
        },
        "chunking": {
            "strategy": "sentence_based_with_overlap",
            "max_chunk_words": config.max_chunk_words,
            "chunk_overlap_words": config.chunk_overlap_words,
            "chunk_count": len(chunk_records),
        },
        "payload": {
            "formats": ["alpaca", "chat"],
            "total_records": len(alpaca_records),
            "task_counts": task_counts,
            "train_records": len(train_ids),
            "val_records": len(val_ids),
            "train_ratio": config.train_ratio,
            "split_seed": config.split_seed,
        },
        "files": {
            "alpaca_all": str(config.alpaca_all_path),
            "alpaca_train": str(config.alpaca_train_path),
            "alpaca_val": str(config.alpaca_val_path),
            "chat_all": str(config.chat_all_path),
            "chat_train": str(config.chat_train_path),
            "chat_val": str(config.chat_val_path),
        },
        "validation": {
            "min_input_words": config.min_input_words,
            "min_output_words": config.min_output_words,
            "required_keys": ["instruction", "input", "output", "messages"],
        },
    }


def run_pipeline() -> None:
    """Execute Day 5 chunking and instruction-payload generation workflow."""

    config = load_config()
    config.output_dir.mkdir(parents=True, exist_ok=True)

    source_frame = load_source_frame(config)
    chunk_records = build_chunk_records(config, source_frame)

    alpaca_records = build_alpaca_records(chunk_records)
    chat_records = build_chat_records(alpaca_records)

    validate_alpaca_records(alpaca_records, config)
    validate_chat_records(chat_records)

    all_ids = [record.id for record in alpaca_records]
    train_ids, val_ids = split_ids(all_ids, config.train_ratio, config.split_seed)

    alpaca_all = [record.__dict__ for record in alpaca_records]
    alpaca_train = [record.__dict__ for record in alpaca_records if record.id in train_ids]
    alpaca_val = [record.__dict__ for record in alpaca_records if record.id in val_ids]

    chat_all = [record.__dict__ for record in chat_records]
    chat_train = [record.__dict__ for record in chat_records if record.id in train_ids]
    chat_val = [record.__dict__ for record in chat_records if record.id in val_ids]

    write_jsonl(config.alpaca_all_path, alpaca_all)
    write_jsonl(config.alpaca_train_path, alpaca_train)
    write_jsonl(config.alpaca_val_path, alpaca_val)
    write_jsonl(config.chat_all_path, chat_all)
    write_jsonl(config.chat_train_path, chat_train)
    write_jsonl(config.chat_val_path, chat_val)

    manifest = build_manifest(
        config,
        source_rows=int(len(source_frame)),
        chunk_records=chunk_records,
        alpaca_records=alpaca_records,
        train_ids=train_ids,
        val_ids=val_ids,
    )
    config.manifest_path.write_text(
        json.dumps(manifest, indent=2, ensure_ascii=True, default=str),
        encoding="utf-8",
    )

    LOGGER.info(
        "Day 5 payload generation completed",
        extra={
            "context": {
                "source_rows": int(len(source_frame)),
                "chunk_count": len(chunk_records),
                "payload_records": len(alpaca_records),
                "alpaca_train_path": str(config.alpaca_train_path),
                "alpaca_val_path": str(config.alpaca_val_path),
                "chat_train_path": str(config.chat_train_path),
                "chat_val_path": str(config.chat_val_path),
                "manifest_path": str(config.manifest_path),
            }
        },
    )


if __name__ == "__main__":
    run_pipeline()
