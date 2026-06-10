"""Compatibility entrypoint for the Day 3 benchmark pipeline.

The teachable pipeline flow now lives in `day3/lesson.py`. This file keeps the
original command working:

    python day3/day3_postgres_to_csv_parquet_benchmark.py
"""

from __future__ import annotations

import sys
from pathlib import Path

if __package__ is None or __package__ == "":
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from day3.lesson import main


if __name__ == "__main__":
    main()
