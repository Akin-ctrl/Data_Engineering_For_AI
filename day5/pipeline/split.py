"""Deterministic train/validation splitting for Day 5 payloads."""

from __future__ import annotations

import random


def split_ids(ids: list[str], train_ratio: float, split_seed: int) -> tuple[set[str], set[str]]:
    """Split ids deterministically into train and validation sets."""

    shuffled = ids.copy()
    random.Random(split_seed).shuffle(shuffled)
    split_index = int(len(shuffled) * train_ratio)

    train_ids = set(shuffled[:split_index])
    val_ids = set(shuffled[split_index:])

    if not train_ids or not val_ids:
        raise ValueError(
            "Train/val split produced an empty subset. "
            "Increase data size or adjust DAY5_TRAIN_RATIO."
        )

    return train_ids, val_ids
