# Day 2 Lab: ArXiv API to PostgreSQL (Live Pagination Pipeline)

## Goal

Pull papers from the live ArXiv API across multiple AI-related categories, page through the Atom feed, flatten each entry into an analytical paper record, and load the result into PostgreSQL with idempotent upserts.

What makes this lab different from Day 1:

- The source is a live API, not a static file.
- The API returns Atom/XML, so the script parses nested feed entries before flattening them.
- Authors are normalized into a separate child table.
- Pagination state is stored in PostgreSQL with a simple watermark table.
- The script sleeps between requests to stay polite to the public API.

## What This Day Covers

- Requesting live data from ArXiv.
- Reading and flattening nested feed entries.
- Normalizing repeated authors into a separate table.
- Using pagination with a persisted watermark.
- Writing raw, clean, and rejected rows idempotently to PostgreSQL.
- Running post-load checks so learners can verify the pipeline worked.

## Files

- `day2_arxiv_api_to_postgres.py`: one-shot Day 2 pipeline script.
- `DAY2_CODE_WALKTHROUGH.md`: beginner-friendly explanation of the script.
- `output/`: rejected sample output and any teaching artifacts created by the pipeline.

## Required Setup

1. Copy `.env.example` to `.env`.
2. Update the PostgreSQL values if needed.
3. Start PostgreSQL with Docker Compose.
4. Install Python dependencies.

## Start PostgreSQL

```bash
docker compose up -d
```

## Install Dependencies

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Run the Day 2 Pipeline

```bash
python day2/day2_arxiv_api_to_postgres.py
```

## Target Database Objects

Schema and tables created automatically in `training_data`:

- `training_data.raw_arxiv_entries`
- `training_data.clean_papers`
- `training_data.paper_authors`
- `training_data.rejected_arxiv_entries`
- `training_data.pipeline_state`

## Defensive Rules Implemented

- Reject entries with missing paper id, title, summary, published date, or authors.
- Normalize the ArXiv id so idempotency survives repeated runs and feed overlap.
- Store the repeated author list in a normalized child table.
- Keep the original nested entry payload for traceability.
- Upsert raw, clean, author, rejected, and state records.

## Outputs You Should Inspect

- JSON logs in the console.
- `day2/output/rejected_sample_<batch_id>.csv`.
- PostgreSQL row counts in raw, clean, authors, rejected, and state tables.
- The persisted watermark in `training_data.pipeline_state`.

## Recommended Day 2 Config (10k+ Papers)

Use these values in `.env` to build a deeper training corpus:

```env
ARXIV_CATEGORIES=cs.LG,cs.AI,cs.CV,cs.CL,stat.ML,cs.RO,cs.IR,cs.CR
ARXIV_SEARCH_QUERY=
ARXIV_SOURCE_NAME=arxiv_multi_domain
ARXIV_MAX_PAPERS=10000
ARXIV_PAGE_SIZE=100
ARXIV_SLEEP_SECONDS=5
ARXIV_DEFAULT_LOOKBACK_DAYS=180
ARXIV_OVERLAP_MINUTES=10
ARXIV_REQUEST_TIMEOUT_SECONDS=90
ARXIV_REQUEST_MAX_RETRIES=6
ARXIV_RETRY_BACKOFF_SECONDS=5
ARXIV_USER_AGENT=DataEngineeringForAI-Day2/1.0 (contact: instructor@example.com)
```

When `ARXIV_SEARCH_QUERY` is blank, the script automatically builds a multi-category query from `ARXIV_CATEGORIES`.

## Notes for Teaching

- The ArXiv feed is Atom/XML, so this lab teaches how to work with live structured feeds rather than files.
- The pagination watermark is intentionally simple: it uses the latest published timestamp plus a small overlap window.
- Later lessons can refactor this into a multi-module package or a reusable CLI.