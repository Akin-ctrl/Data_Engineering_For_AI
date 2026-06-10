"""JSONL and manifest output helpers for Day 5."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from day5.pipeline.config import Day5Config
from day5.pipeline.models import AlpacaRecord, ChatRecord, ChunkRecord


def write_jsonl(path: Path, records: list[dict[str, Any]]) -> None:
    """Write newline-delimited JSON records deterministically."""

    with path.open("w", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record, ensure_ascii=True, default=str))
            handle.write("\n")


def split_payloads(
    alpaca_records: list[AlpacaRecord],
    chat_records: list[ChatRecord],
    train_ids: set[str],
    val_ids: set[str],
) -> tuple[
    list[dict[str, Any]],
    list[dict[str, Any]],
    list[dict[str, Any]],
    list[dict[str, Any]],
    list[dict[str, Any]],
    list[dict[str, Any]],
]:
    """Create all/train/validation payload lists for both output formats."""

    alpaca_all = [record.__dict__ for record in alpaca_records]
    alpaca_train = [record.__dict__ for record in alpaca_records if record.id in train_ids]
    alpaca_val = [record.__dict__ for record in alpaca_records if record.id in val_ids]

    chat_all = [record.__dict__ for record in chat_records]
    chat_train = [record.__dict__ for record in chat_records if record.id in train_ids]
    chat_val = [record.__dict__ for record in chat_records if record.id in val_ids]

    return alpaca_all, alpaca_train, alpaca_val, chat_all, chat_train, chat_val


def write_payload_files(
    config: Day5Config,
    alpaca_all: list[dict[str, Any]],
    alpaca_train: list[dict[str, Any]],
    alpaca_val: list[dict[str, Any]],
    chat_all: list[dict[str, Any]],
    chat_train: list[dict[str, Any]],
    chat_val: list[dict[str, Any]],
) -> None:
    """Write all Day 5 JSONL payload files."""

    write_jsonl(config.alpaca_all_path, alpaca_all)
    write_jsonl(config.alpaca_train_path, alpaca_train)
    write_jsonl(config.alpaca_val_path, alpaca_val)
    write_jsonl(config.chat_all_path, chat_all)
    write_jsonl(config.chat_train_path, chat_train)
    write_jsonl(config.chat_val_path, chat_val)


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


def write_manifest(config: Day5Config, manifest: dict[str, Any]) -> None:
    """Write the Day 5 manifest file."""

    config.manifest_path.write_text(
        json.dumps(manifest, indent=2, ensure_ascii=True, default=str),
        encoding="utf-8",
    )
