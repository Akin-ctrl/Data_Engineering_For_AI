"""File output helpers for Day 1."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd


def export_rejected_csv_sample(df_reject: pd.DataFrame, batch_id: str) -> Path:
    """Persist a small rejected sample for teaching and troubleshooting."""

    output_dir = Path(__file__).resolve().parents[1] / "output"
    output_dir.mkdir(parents=True, exist_ok=True)

    output_path = output_dir / f"rejected_sample_{batch_id}.csv"
    sample_df = df_reject.copy()
    sample_df["raw_payload"] = sample_df["raw_payload"].apply(
        lambda payload: json.dumps(payload, ensure_ascii=True)
    )
    sample_df.head(100).to_csv(output_path, index=False)
    return output_path
