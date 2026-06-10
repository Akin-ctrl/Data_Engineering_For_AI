"""Validation and clean/reject splitting for ArXiv records."""

from __future__ import annotations

from typing import Any

from day2.pipeline.parse import stable_hash, strip_text


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
