"""Tests for the Day 5 train and validation split.

The split has to be repeatable. If the same seed gave a different split each
run, you could not compare two training runs against each other.
"""

from __future__ import annotations

import pytest

from day5.pipeline.split import split_ids


def make_ids(count: int) -> list[str]:
    """Build a predictable list of record ids."""

    return [f"id-{number:04d}" for number in range(count)]


class TestSplitIds:
    """Splitting is deterministic, complete, and non-overlapping."""

    def test_same_seed_gives_the_same_split(self) -> None:
        ids = make_ids(100)
        first_train, first_val = split_ids(ids, train_ratio=0.9, split_seed=42)
        second_train, second_val = split_ids(ids, train_ratio=0.9, split_seed=42)
        assert first_train == second_train
        assert first_val == second_val

    def test_a_different_seed_gives_a_different_split(self) -> None:
        ids = make_ids(100)
        train_a, _ = split_ids(ids, train_ratio=0.9, split_seed=42)
        train_b, _ = split_ids(ids, train_ratio=0.9, split_seed=7)
        assert train_a != train_b

    def test_no_id_lands_in_both_sets(self) -> None:
        train, val = split_ids(make_ids(100), train_ratio=0.9, split_seed=42)
        assert not (train & val)

    def test_every_id_is_accounted_for(self) -> None:
        ids = make_ids(100)
        train, val = split_ids(ids, train_ratio=0.9, split_seed=42)
        assert train | val == set(ids)

    @pytest.mark.parametrize("train_ratio", [0.5, 0.8, 0.9])
    def test_the_ratio_is_respected(self, train_ratio: float) -> None:
        ids = make_ids(100)
        train, _ = split_ids(ids, train_ratio=train_ratio, split_seed=42)
        assert len(train) == int(100 * train_ratio)

    def test_input_list_is_not_modified(self) -> None:
        ids = make_ids(50)
        original = ids.copy()
        split_ids(ids, train_ratio=0.9, split_seed=42)
        assert ids == original

    def test_ratio_that_would_empty_validation_raises(self) -> None:
        with pytest.raises(ValueError, match="empty subset"):
            split_ids(make_ids(10), train_ratio=1.0, split_seed=42)

    def test_ratio_that_would_empty_training_raises(self) -> None:
        with pytest.raises(ValueError, match="empty subset"):
            split_ids(make_ids(10), train_ratio=0.0, split_seed=42)

    def test_too_few_ids_to_split_raises(self) -> None:
        with pytest.raises(ValueError, match="empty subset"):
            split_ids(["only-one"], train_ratio=0.9, split_seed=42)
