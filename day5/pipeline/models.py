"""Small data models used by the Day 5 payload pipeline."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


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
