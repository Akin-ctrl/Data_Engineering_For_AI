"""Transform and validation helpers for Day 1 reviews."""

from __future__ import annotations

import hashlib
import json
import re

import pandas as pd


EXPECTED_COLUMNS = [
    "unnamed_0",
    "clothing_id",
    "age",
    "title",
    "review_text",
    "rating",
    "recommended_ind",
    "positive_feedback_count",
    "division_name",
    "department_name",
    "class_name",
]


def normalize_column_name(column_name: str) -> str:
    """Normalize a column name to safe snake_case."""

    lowered = column_name.strip().lower()
    normalized = re.sub(r"[^a-z0-9]+", "_", lowered)
    normalized = re.sub(r"_+", "_", normalized).strip("_")
    return normalized


def stable_row_hash(row: pd.Series) -> str:
    """Create a deterministic hash for a row after string normalization."""

    normalized = {
        str(key): "" if pd.isna(value) else str(value).strip()
        for key, value in row.to_dict().items()
    }
    row_json = json.dumps(normalized, sort_keys=True, ensure_ascii=True)
    return hashlib.sha256(row_json.encode("utf-8")).hexdigest()


def prepare_raw_records_for_load(df_raw: pd.DataFrame) -> pd.DataFrame:
    """Normalize raw rows and attach row metadata before database loading."""

    df_normalized = df_raw.copy()
    df_normalized.columns = [normalize_column_name(col) for col in df_normalized.columns]
    df_normalized = df_normalized.reset_index(drop=True)
    df_normalized["row_number"] = df_normalized.index + 1
    df_normalized["row_hash"] = df_normalized.apply(stable_row_hash, axis=1)
    return df_normalized


def clean_dataset_defensively(df_raw: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Apply defensive cleaning and split records into clean and rejected sets."""

    df = df_raw.copy()
    df.columns = [normalize_column_name(col) for col in df.columns]

    df = df.reset_index(drop=True)
    df["row_number"] = df.index + 1
    df["row_hash"] = df.apply(stable_row_hash, axis=1)

    missing_columns = [col for col in EXPECTED_COLUMNS if col not in df.columns]
    if missing_columns:
        raise ValueError(
            "Dataset does not match expected schema. Missing columns: "
            + ", ".join(missing_columns)
        )

    df["review_id"] = pd.to_numeric(df["unnamed_0"], errors="coerce")
    df["clothing_id"] = pd.to_numeric(df["clothing_id"], errors="coerce")
    df["age"] = pd.to_numeric(df["age"], errors="coerce")
    df["rating"] = pd.to_numeric(df["rating"], errors="coerce")
    df["recommended_ind"] = pd.to_numeric(df["recommended_ind"], errors="coerce")
    df["positive_feedback_count"] = pd.to_numeric(df["positive_feedback_count"], errors="coerce")

    for text_col in [
        "title",
        "review_text",
        "division_name",
        "department_name",
        "class_name",
    ]:
        df[text_col] = df[text_col].astype(str).str.strip()

    df["review_key"] = (
        df["review_id"].astype("Int64").astype(str).where(df["review_id"].notna(), df["row_hash"])
    )

    reasons: list[list[str]] = []
    for _, row in df.iterrows():
        row_reasons: list[str] = []

        if pd.isna(row["clothing_id"]):
            row_reasons.append("missing_or_invalid_clothing_id")
        if pd.isna(row["rating"]):
            row_reasons.append("missing_or_invalid_rating")
        elif int(row["rating"]) < 1 or int(row["rating"]) > 5:
            row_reasons.append("rating_out_of_range_1_to_5")
        if pd.isna(row["recommended_ind"]):
            row_reasons.append("missing_or_invalid_recommended_ind")
        elif int(row["recommended_ind"]) not in {0, 1}:
            row_reasons.append("recommended_ind_not_binary")
        if pd.isna(row["positive_feedback_count"]):
            row_reasons.append("missing_or_invalid_positive_feedback_count")
        elif int(row["positive_feedback_count"]) < 0:
            row_reasons.append("negative_positive_feedback_count")

        review_text = str(row["review_text"]).strip().lower()
        if not review_text or review_text in {"nan", "none"}:
            row_reasons.append("missing_review_text")

        if not pd.isna(row["age"]):
            age_value = int(row["age"])
            if age_value < 13 or age_value > 100:
                row_reasons.append("age_out_of_expected_range")

        reasons.append(row_reasons)

    df["reject_reason"] = [";".join(reason_list) for reason_list in reasons]
    is_rejected = df["reject_reason"] != ""

    raw_payload = df_raw.copy().astype(object)
    raw_payload.columns = [normalize_column_name(col) for col in df_raw.columns]
    raw_payload = raw_payload.where(pd.notna(raw_payload), None)
    df["raw_payload"] = raw_payload.to_dict(orient="records")

    df_clean = df.loc[~is_rejected].copy()
    df_reject = df.loc[is_rejected].copy()

    clean_columns = [
        "review_key",
        "row_hash",
        "row_number",
        "review_id",
        "clothing_id",
        "age",
        "title",
        "review_text",
        "rating",
        "recommended_ind",
        "positive_feedback_count",
        "division_name",
        "department_name",
        "class_name",
        "raw_payload",
    ]
    reject_columns = ["row_hash", "row_number", "reject_reason", "raw_payload"]

    return df_clean[clean_columns], df_reject[reject_columns]
