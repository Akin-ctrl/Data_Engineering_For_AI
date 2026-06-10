"""File output helpers for Day 2."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any


def export_rejected_csv_sample(rejected_entries: list[dict[str, Any]], batch_id: str) -> Path:
    """Export a small CSV sample of rejected rows for classroom inspection."""

    output_dir = Path(__file__).resolve().parents[1] / "output"
    output_dir.mkdir(parents=True, exist_ok=True)

    output_path = output_dir / f"rejected_sample_{batch_id}.csv"
    sample_rows = []
    for entry in rejected_entries[:100]:
        sample_rows.append(
            {
                "paper_key": entry.get("paper_key"),
                "row_hash": entry.get("row_hash"),
                "reason": entry.get("reason"),
                "raw_payload": json.dumps(entry.get("raw_payload", {}), ensure_ascii=True, default=str),
            }
        )

    if sample_rows:
        with output_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(sample_rows[0].keys()))
            writer.writeheader()
            writer.writerows(sample_rows)

    return output_path
