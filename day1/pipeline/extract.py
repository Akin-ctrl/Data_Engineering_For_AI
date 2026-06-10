"""Extract helpers for downloading and reading the Day 1 CSV."""

from __future__ import annotations

import re
from collections.abc import Iterable
from pathlib import Path
from typing import Any
from urllib.parse import unquote

import pandas as pd
import requests

from day1.pipeline.config import PipelineConfig
from day1.pipeline.logging_utils import LOGGER


def parse_hf_dataset_id(url: str) -> str | None:
    """Extract dataset identifier owner/name from a Hugging Face dataset URL."""

    match = re.search(r"huggingface\.co/datasets/([^/]+/[^/?#]+)", url)
    if not match:
        return None
    return unquote(match.group(1))


def resolve_huggingface_csv_url(input_url: str) -> str:
    """Resolve a Hugging Face dataset page URL into a direct CSV download URL."""

    if "/resolve/" in input_url and input_url.lower().endswith(".csv"):
        return input_url

    dataset_id = parse_hf_dataset_id(input_url)
    if not dataset_id:
        raise ValueError("HF_CSV_URL is not a valid Hugging Face dataset URL.")

    api_url = f"https://huggingface.co/api/datasets/{dataset_id}"
    response = requests.get(api_url, timeout=30)
    response.raise_for_status()
    metadata = response.json()

    siblings: Iterable[dict[str, Any]] = metadata.get("siblings", [])
    csv_file = next(
        (
            sibling.get("rfilename")
            for sibling in siblings
            if str(sibling.get("rfilename", "")).lower().endswith(".csv")
        ),
        None,
    )
    if not csv_file:
        raise ValueError(f"No CSV file found in dataset repository: {dataset_id}")

    encoded_filename = csv_file.replace(" ", "%20")
    return f"https://huggingface.co/datasets/{dataset_id}/resolve/main/{encoded_filename}"


def download_csv(url: str, destination: Path) -> Path:
    """Download a CSV file from URL to destination path."""

    destination.parent.mkdir(parents=True, exist_ok=True)
    response = requests.get(url, timeout=60)
    response.raise_for_status()
    destination.write_bytes(response.content)
    return destination


def download_reviews_csv(config: PipelineConfig, batch_id: str, repo_root: Path) -> tuple[str, Path]:
    """Resolve and download the configured Hugging Face CSV for one batch."""

    raw_dir = repo_root / "day1" / "output"
    raw_path = raw_dir / f"raw_download_{batch_id}.csv"
    resolved_source_url = resolve_huggingface_csv_url(config.hf_csv_url)
    downloaded_path = download_csv(resolved_source_url, raw_path)

    LOGGER.info(
        "CSV downloaded",
        extra={
            "context": {
                "batch_id": batch_id,
                "csv_path": str(downloaded_path),
                "resolved_source_url": resolved_source_url,
            }
        },
    )
    return resolved_source_url, downloaded_path


def read_reviews_csv(csv_path: Path) -> pd.DataFrame:
    """Read CSV with resilient options suited for beginner pipelines."""

    return pd.read_csv(
        csv_path,
        dtype=str,
        keep_default_na=True,
        na_values=["", "null", "NULL", "None", "none", "NaN", "nan"],
        on_bad_lines="skip",
        encoding="utf-8",
    )
