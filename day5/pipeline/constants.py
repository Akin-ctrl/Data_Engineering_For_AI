"""Shared constants for the Day 5 instruction payload pipeline."""

import re


DEFAULT_SOURCE_TABLE = "training_data.clean_papers"
DEFAULT_ORDER_BY = "paper_key"
DEFAULT_OUTPUT_DIR = "day5/output"
DEFAULT_EXPORT_BASENAME = "day5_instruction_payload"
DEFAULT_MAX_CHUNK_WORDS = 350
DEFAULT_CHUNK_OVERLAP_WORDS = 50
DEFAULT_MIN_INPUT_WORDS = 30
DEFAULT_MIN_OUTPUT_WORDS = 3
DEFAULT_TRAIN_RATIO = 0.9
DEFAULT_SPLIT_SEED = 42
DEFAULT_MAX_PAPERS = 0
IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)*$")
SIMPLE_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
EXPORT_BASENAME_PATTERN = re.compile(r"^[A-Za-z0-9_-]+$")
