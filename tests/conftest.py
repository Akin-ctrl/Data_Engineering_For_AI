"""Shared pytest setup for the lab test suite.

The day packages are imported as `day1.pipeline...`, so the repository root has
to be on the import path when pytest runs from anywhere.
"""

from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]

if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
