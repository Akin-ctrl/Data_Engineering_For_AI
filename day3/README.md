# Day 3 Lab: PostgreSQL to CSV and Parquet Benchmark

## Goal

Turn the clean Day 2 data in PostgreSQL into two export formats, then prove the difference with a simple benchmark.

This lab is designed to make the storage tradeoff obvious:

- CSV is easy to inspect and share.
- Parquet is columnar, compact, and faster to read for analytical workloads.
- The benchmark makes the difference measurable in milliseconds and megabytes.

## What This Day Covers

- Querying the clean Day 2 table from PostgreSQL.
- Exporting the same dataset to CSV and Parquet.
- Using PyArrow to write Parquet with `snappy` compression.
- Reading CSV and Parquet with both pandas and PyArrow.
- Measuring file size and read-time differences.
- Writing a JSON benchmark report for later review.

## Files

- `day3_postgres_to_csv_parquet_benchmark.py`: the Day 3 pipeline script.
- `DAY3_CODE_WALKTHROUGH.md`: beginner-friendly explanation of the script.
- `day3_agent_query_views.sql`: SQL views and materialized views for agent-style querying.
- `output/`: deterministic CSV, Parquet, and JSON benchmark artifacts.

## Required Setup

1. Copy `.env.example` to `.env` if you have not already.
2. Confirm PostgreSQL is running with the Day 2 data loaded.
3. Install the Python dependencies.

## Install Dependencies

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Run the Day 3 Pipeline

```bash
python day3/day3_postgres_to_csv_parquet_benchmark.py
```

## Default Source and Outputs

By default, the script reads from `training_data.clean_papers` and writes these files:

- `day3/output/day3_clean_papers_benchmark.csv`
- `day3/output/day3_clean_papers_benchmark.parquet`
- `day3/output/day3_clean_papers_benchmark_benchmark.json`

The files are overwritten deterministically on each run.

## Environment Variables

Recommended Day 3 settings:

```env
DAY3_SOURCE_TABLE=training_data.clean_papers
DAY3_ORDER_BY=paper_key
DAY3_OUTPUT_DIR=day3/output
DAY3_EXPORT_BASENAME=day3_clean_papers_benchmark
DAY3_BENCHMARK_RUNS=10
```

## What To Look At

- The console summary table.
- The JSON benchmark report in `day3/output/`.
- The size gap between the CSV and Parquet files.
- The read-time difference between pandas and PyArrow on both formats.

## Agent Query Views

If you want an AI agent to answer user questions with fast SQL, create the Day 3 views:

```bash
eval "$(grep -E '^(PGHOST|PGPORT|PGDATABASE|PGUSER|PGPASSWORD)=' .env | sed 's/^/export /')"
psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" -f day3/day3_agent_query_views.sql
```

After each new ingestion batch, refresh the materialized views:

```sql
REFRESH MATERIALIZED VIEW training_data.mv_agent_keyword_frequency;
REFRESH MATERIALIZED VIEW training_data.mv_agent_category_cooccurrence;
```

Recommended agent-first tables/views:

- `training_data.v_agent_papers`
- `training_data.v_agent_category_counts`
- `training_data.v_agent_monthly_category_counts`
- `training_data.v_agent_author_frequency`
- `training_data.v_agent_author_category_frequency`
- `training_data.v_agent_metadata_quality`
- `training_data.v_agent_pipeline_health`
- `training_data.v_agent_recent_papers`
- `training_data.mv_agent_keyword_frequency`
- `training_data.mv_agent_category_cooccurrence`


