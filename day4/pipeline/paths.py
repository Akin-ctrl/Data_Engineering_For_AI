"""Path validation helpers for Day 4."""

from __future__ import annotations

from day4.pipeline.constants import DAY2_SCRIPT, DAY3_SCRIPT, DAY3_VIEWS_SQL


def ensure_paths_exist() -> None:
    """Fail fast if the reusable lab scripts are missing."""

    missing_paths = [path for path in [DAY2_SCRIPT, DAY3_VIEWS_SQL, DAY3_SCRIPT] if not path.exists()]
    if missing_paths:
        raise FileNotFoundError(f"Missing required lab files: {', '.join(str(path) for path in missing_paths)}")
