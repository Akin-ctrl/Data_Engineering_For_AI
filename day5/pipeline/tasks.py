"""Instruction task builders for Day 5 payloads."""

from __future__ import annotations

from day5.pipeline.models import AlpacaRecord, ChatRecord, ChunkRecord
from day5.pipeline.text import split_sentences


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
