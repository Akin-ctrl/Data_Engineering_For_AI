"""Text normalization and chunking helpers for Day 5."""

from __future__ import annotations

import hashlib
import re

import pandas as pd

from day5.pipeline.config import Day5Config
from day5.pipeline.models import ChunkRecord


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
