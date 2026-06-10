"""Shared constants for the Day 2 ArXiv pipeline."""

SCHEMA_NAME = "training_data"
RAW_TABLE = "raw_arxiv_entries"
CLEAN_TABLE = "clean_papers"
AUTHOR_TABLE = "paper_authors"
REJECT_TABLE = "rejected_arxiv_entries"
STATE_TABLE = "pipeline_state"

ARXIV_NS = "http://www.w3.org/2005/Atom"
OPENSEARCH_NS = "http://a9.com/-/spec/opensearch/1.1/"
ARXIV_META_NS = "http://arxiv.org/schemas/atom"
NAMESPACES = {
    "atom": ARXIV_NS,
    "opensearch": OPENSEARCH_NS,
    "arxiv": ARXIV_META_NS,
}

DEFAULT_ARXIV_CATEGORIES = [
    "cs.LG",
    "cs.AI",
    "cs.CV",
    "cs.CL",
    "stat.ML",
    "cs.RO",
    "cs.IR",
    "cs.CR",
]
