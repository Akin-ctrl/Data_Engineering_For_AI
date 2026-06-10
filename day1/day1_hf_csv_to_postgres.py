"""Compatibility entrypoint for the Day 1 pipeline.

The teachable pipeline flow now lives in `day1/lesson.py`. This file keeps the
original command working:

    python day1/day1_hf_csv_to_postgres.py
"""

from __future__ import annotations

import sys
from pathlib import Path

if __package__ is None or __package__ == "":
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from day1.lesson import main


if __name__ == "__main__":
    main()
