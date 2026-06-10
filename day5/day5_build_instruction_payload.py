"""Compatibility entrypoint for the Day 5 instruction payload pipeline.

The teachable payload-building flow now lives in `day5/lesson.py`. This file
keeps the original command working:

    python day5/day5_build_instruction_payload.py
"""

from __future__ import annotations

import sys
from pathlib import Path

if __package__ is None or __package__ == "":
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from day5.lesson import main


if __name__ == "__main__":
    main()
