"""Shared constants for the Day 3 benchmark pipeline."""

import re
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
DAY3_VIEWS_SQL = REPO_ROOT / "day3" / "day3_agent_query_views.sql"
SCHEMA_NAME = "training_data"
DEFAULT_SOURCE_TABLE = "training_data.clean_papers"
DEFAULT_ORDER_BY = "paper_key"
DEFAULT_OUTPUT_DIR = "day3/output"
DEFAULT_BENCHMARK_RUNS = 10
DEFAULT_EXPORT_BASENAME = "day3_clean_papers_benchmark"
IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)*$")
SIMPLE_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
EXPORT_BASENAME_PATTERN = re.compile(r"^[A-Za-z0-9_-]+$")

# Read modes used by the benchmark. "full" reads every column, "projected"
# reads only the few columns in PROJECTION_COLUMNS.
READ_MODE_FULL = "full"
READ_MODE_PROJECTED = "projected"
READ_MODES = [READ_MODE_FULL, READ_MODE_PROJECTED]

# A small, cheap slice of the export. These three columns are tiny next to
# summary and raw_payload, which is the point: CSV still has to read and parse
# every byte of every row to find them, while Parquet reads only these columns.
PROJECTION_COLUMNS = ["paper_key", "primary_category", "author_count"]
