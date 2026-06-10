"""Compatibility entrypoint for the Day 4 orchestrated workflow.

The teachable workflow flow now lives in `day4/lesson.py`. This file keeps the
original command working:

    python day4/day4_orchestrated_workflow.py
"""

from __future__ import annotations

import sys
from pathlib import Path

if __package__ is None or __package__ == "":
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from day4.lesson import main


if __name__ == "__main__":
    main()
