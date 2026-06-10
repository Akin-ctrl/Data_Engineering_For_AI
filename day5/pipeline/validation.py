"""Validation helpers for Day 5 payload records."""

from __future__ import annotations

from statistics import mean

from day5.pipeline.config import Day5Config
from day5.pipeline.logging_utils import LOGGER
from day5.pipeline.models import AlpacaRecord, ChatRecord
from day5.pipeline.text import word_count


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
