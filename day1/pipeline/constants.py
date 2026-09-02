"""Shared constants for the Day 1 reviews pipeline.

Day 1 is a standalone lab, so it keeps its own schema. Days 2 to 5 build one
chained pipeline and share the `training_data` schema instead.
"""

SCHEMA_NAME = "de_ai"
RAW_TABLE = "raw_reviews"
CLEAN_TABLE = "clean_reviews"
REJECT_TABLE = "rejected_reviews"
