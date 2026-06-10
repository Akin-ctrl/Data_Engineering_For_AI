"""Shared constants for the Day 3 benchmark pipeline."""

import re


SCHEMA_NAME = "training_data"
DEFAULT_SOURCE_TABLE = "training_data.clean_papers"
DEFAULT_ORDER_BY = "paper_key"
DEFAULT_OUTPUT_DIR = "day3/output"
DEFAULT_BENCHMARK_RUNS = 10
DEFAULT_EXPORT_BASENAME = "day3_clean_papers_benchmark"
IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)*$")
SIMPLE_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
EXPORT_BASENAME_PATTERN = re.compile(r"^[A-Za-z0-9_-]+$")
