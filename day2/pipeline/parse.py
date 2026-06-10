"""ArXiv Atom/XML parsing helpers."""

from __future__ import annotations

import hashlib
import json
import re
import xml.etree.ElementTree as ET
from datetime import datetime
from typing import Any

from day2.pipeline.constants import ARXIV_META_NS, ARXIV_NS


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
