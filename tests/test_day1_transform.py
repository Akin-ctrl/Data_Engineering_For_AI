"""Tests for the Day 1 cleaning and transform helpers.

These are the rules the Day 1 lab teaches: normalise the column names, give
every row a stable fingerprint, and split rows into clean and rejected without
losing any of them.
"""

from __future__ import annotations

import pandas as pd
import pytest

from day1.pipeline.transform import (
    EXPECTED_COLUMNS,
    clean_dataset_defensively,
    normalize_column_name,
    stable_row_hash,
)


def build_valid_row(**overrides: object) -> dict[str, object]:
    """Build one row that passes every Day 1 rule, with optional overrides."""

    row: dict[str, object] = {
        "Unnamed: 0": 1,
        "Clothing ID": 767,
        "Age": 33,
        "Title": "Nice dress",
        "Review Text": "The fabric is soft and it fits well.",
        "Rating": 4,
        "Recommended IND": 1,
        "Positive Feedback Count": 2,
        "Division Name": "General",
        "Department Name": "Dresses",
        "Class Name": "Dresses",
    }
    row.update(overrides)
    return row


def build_frame(rows: list[dict[str, object]]) -> pd.DataFrame:
    """Build a raw DataFrame shaped like the downloaded review CSV."""

    return pd.DataFrame(rows)


class TestNormalizeColumnName:
    """Column names must end up as safe snake_case."""

    @pytest.mark.parametrize(
        ("raw_name", "expected"),
        [
            ("Unnamed: 0", "unnamed_0"),
            ("Positive Feedback Count", "positive_feedback_count"),
            ("  Review Text  ", "review_text"),
            ("Division-Name", "division_name"),
            ("Class   Name", "class_name"),
            ("ALREADY_SNAKE", "already_snake"),
        ],
    )
    def test_normalizes_expected_names(self, raw_name: str, expected: str) -> None:
        assert normalize_column_name(raw_name) == expected

    def test_strips_leading_and_trailing_separators(self) -> None:
        assert normalize_column_name("__weird name__") == "weird_name"

    def test_collapses_repeated_separators(self) -> None:
        assert normalize_column_name("a...b") == "a_b"


class TestStableRowHash:
    """The row hash is how Day 1 recognises the same row twice."""

    def test_same_values_give_same_hash(self) -> None:
        first = pd.Series({"a": "1", "b": "x"})
        second = pd.Series({"a": "1", "b": "x"})
        assert stable_row_hash(first) == stable_row_hash(second)

    def test_key_order_does_not_change_the_hash(self) -> None:
        first = pd.Series({"a": "1", "b": "x"})
        second = pd.Series({"b": "x", "a": "1"})
        assert stable_row_hash(first) == stable_row_hash(second)

    def test_surrounding_spaces_do_not_change_the_hash(self) -> None:
        first = pd.Series({"a": "1", "b": "  x  "})
        second = pd.Series({"a": "1", "b": "x"})
        assert stable_row_hash(first) == stable_row_hash(second)

    def test_missing_values_are_treated_as_empty(self) -> None:
        first = pd.Series({"a": "1", "b": None})
        second = pd.Series({"a": "1", "b": ""})
        assert stable_row_hash(first) == stable_row_hash(second)

    def test_different_values_give_different_hashes(self) -> None:
        first = pd.Series({"a": "1", "b": "x"})
        second = pd.Series({"a": "2", "b": "x"})
        assert stable_row_hash(first) != stable_row_hash(second)


class TestCleanDatasetDefensively:
    """Cleaning splits rows into clean and rejected, and never drops any."""

    def test_valid_row_is_kept(self) -> None:
        frame = build_frame([build_valid_row()])
        clean, rejected = clean_dataset_defensively(frame)
        assert len(clean) == 1
        assert len(rejected) == 0

    def test_no_row_is_lost(self) -> None:
        frame = build_frame(
            [
                build_valid_row(),
                build_valid_row(Rating=9),
                build_valid_row(**{"Review Text": ""}),
                build_valid_row(**{"Clothing ID": None}),
            ]
        )
        clean, rejected = clean_dataset_defensively(frame)
        assert len(clean) + len(rejected) == len(frame)

    @pytest.mark.parametrize("rating", [1, 2, 3, 4, 5])
    def test_ratings_inside_the_allowed_range_are_kept(self, rating: int) -> None:
        frame = build_frame([build_valid_row(Rating=rating)])
        clean, _ = clean_dataset_defensively(frame)
        assert len(clean) == 1

    @pytest.mark.parametrize("rating", [0, 6, -1, 100])
    def test_ratings_outside_the_allowed_range_are_rejected(self, rating: int) -> None:
        frame = build_frame([build_valid_row(Rating=rating)])
        _, rejected = clean_dataset_defensively(frame)
        assert len(rejected) == 1
        assert "rating_out_of_range_1_to_5" in rejected.iloc[0]["reject_reason"]

    def test_empty_review_text_is_rejected(self) -> None:
        frame = build_frame([build_valid_row(**{"Review Text": "   "})])
        _, rejected = clean_dataset_defensively(frame)
        assert "missing_review_text" in rejected.iloc[0]["reject_reason"]

    def test_negative_feedback_count_is_rejected(self) -> None:
        frame = build_frame([build_valid_row(**{"Positive Feedback Count": -3})])
        _, rejected = clean_dataset_defensively(frame)
        assert "negative_positive_feedback_count" in rejected.iloc[0]["reject_reason"]

    def test_non_binary_recommended_flag_is_rejected(self) -> None:
        frame = build_frame([build_valid_row(**{"Recommended IND": 7})])
        _, rejected = clean_dataset_defensively(frame)
        assert "recommended_ind_not_binary" in rejected.iloc[0]["reject_reason"]

    @pytest.mark.parametrize("age", [12, 101, 150])
    def test_age_outside_the_expected_range_is_rejected(self, age: int) -> None:
        frame = build_frame([build_valid_row(Age=age)])
        _, rejected = clean_dataset_defensively(frame)
        assert "age_out_of_expected_range" in rejected.iloc[0]["reject_reason"]

    def test_one_row_can_collect_several_reject_reasons(self) -> None:
        frame = build_frame([build_valid_row(Rating=9, **{"Review Text": ""})])
        _, rejected = clean_dataset_defensively(frame)
        reasons = rejected.iloc[0]["reject_reason"]
        assert "rating_out_of_range_1_to_5" in reasons
        assert "missing_review_text" in reasons

    def test_missing_expected_column_raises(self) -> None:
        row = build_valid_row()
        del row["Rating"]
        with pytest.raises(ValueError, match="Missing columns"):
            clean_dataset_defensively(build_frame([row]))

    def test_expected_columns_list_is_not_empty(self) -> None:
        assert EXPECTED_COLUMNS
