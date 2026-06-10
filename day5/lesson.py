"""Teachable Day 5 instruction-payload entrypoint.

Open this file first in class. It shows the payload-building story without
starting inside chunking, validation, JSONL writing, or split internals.
"""

from __future__ import annotations

import sys
from pathlib import Path

if __package__ is None or __package__ == "":
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from day5.pipeline.config import load_config
from day5.pipeline.logging_utils import LOGGER
from day5.pipeline.outputs import build_manifest, split_payloads, write_manifest, write_payload_files
from day5.pipeline.source import load_source_frame
from day5.pipeline.split import split_ids
from day5.pipeline.tasks import build_alpaca_records, build_chat_records
from day5.pipeline.text import build_chunk_records
from day5.pipeline.validation import validate_alpaca_records, validate_chat_records


def run_pipeline() -> None:
    """Run Day 5 as a readable source-chunk-payload-validate-export story."""

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

    payloads = split_payloads(alpaca_records, chat_records, train_ids, val_ids)
    write_payload_files(config, *payloads)

    manifest = build_manifest(
        config,
        source_rows=int(len(source_frame)),
        chunk_records=chunk_records,
        alpaca_records=alpaca_records,
        train_ids=train_ids,
        val_ids=val_ids,
    )
    write_manifest(config, manifest)

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


def main() -> None:
    """Entry point for Day 5 payload generation."""

    run_pipeline()


if __name__ == "__main__":
    main()
