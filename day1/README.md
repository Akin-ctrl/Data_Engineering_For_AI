# Day 1 Lab: HuggingFace CSV to PostgreSQL (Defensive Pipeline)

## Goal

Load a raw CSV dataset from HuggingFace, clean it defensively with pandas, and load it safely into PostgreSQL using structured tables.

Dataset used for Day 1:

- `Censius-AI/ECommerce-Women-Clothing-Reviews`
- Single split: `train` (about 23.5k rows)
- Source CSV: `Womens Clothing E-Commerce Reviews.csv`

## What This Day Covers

- Downloading a public CSV from HuggingFace.
- Reading raw data into memory with pandas.
- Defensive cleaning for production-like failure modes.
- Upsert loading into PostgreSQL.
- Rejected row capture in both PostgreSQL and CSV sample output.
- Post-load checks for row counts, nulls, uniqueness, and sample validation.

## Files

- `day1_hf_csv_to_postgres.py`: one-shot Day 1 pipeline script.
- `DAY1_CODE_WALKTHROUGH.md`: beginner-friendly section-by-section script explanation.
- `output/`: downloaded raw file and rejected sample output files.

## Required Setup

1. Copy `.env.example` to `.env`.
2. Update `.env` values.
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

## Run the Day 1 Pipeline

```bash
python day1/day1_hf_csv_to_postgres.py
```

## Target Database Objects

Schema and tables created automatically:

- `de_ai.raw_reviews`
- `de_ai.clean_reviews`
- `de_ai.rejected_reviews`

## Defensive Rules Implemented

- Strict schema check against expected review columns.
- Column normalization to safe snake_case.
- Type coercion for numeric fields (`rating`, `age`, etc.).
- Reject rows with invalid `rating` (must be 1..5).
- Reject rows with invalid `recommended_ind` (must be 0 or 1).
- Reject rows with negative feedback counts.
- Reject rows with missing review text.
- Keep a full raw payload for traceability.
- Upsert behavior for raw, clean, and rejected tables.

## Outputs You Should Inspect

- JSON logs in console.
- `day1/output/rejected_sample_<batch_id>.csv`.
- PostgreSQL row counts in raw, clean, and rejected tables.
- Rating distribution sample from the clean table in logs.

## Notes for Teaching

- This is intentionally a one-shot script for foundational learning.
- Later days can refactor this into reusable classes and CLI workflows.
