"""Compatibility entrypoint for the Day 2 ArXiv pipeline.

The teachable pipeline flow now lives in `day2/lesson.py`. This file keeps the
original command working:

    python day2/day2_arxiv_api_to_postgres.py
"""

from __future__ import annotations

import sys
from pathlib import Path

if __package__ is None or __package__ == "":
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from day2.lesson import main


if __name__ == "__main__":
    main()
