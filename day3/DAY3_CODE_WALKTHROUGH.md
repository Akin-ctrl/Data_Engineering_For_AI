# Day 3 Code Walkthrough

Day 3 has been refactored so the first file you open in class is small and readable.

Start here:

- `day3/lesson.py`

Use the files inside `day3/pipeline/` only when you want to explain one specific implementation detail.

## Big Picture

Day 3 teaches why file format choices matter for analytical and AI workloads.

```text
PostgreSQL clean table -> CSV export + Parquet export -> read benchmark -> JSON report
```

The important learning idea is:

> The same data can be stored in different formats, and those formats affect size, speed, and downstream usability.

## What Changes From Day 2

Day 2 focused on ingestion.

Day 3 starts from already-clean data and asks:

- How do we export it?
- Which format is smaller?
- Which format reads faster?
- How do we prove the difference?

The pipeline shape is:

```text
configure -> query source -> export files -> benchmark reads -> report results
```

## File Map

| File | What It Explains |
|---|---|
| `day3/lesson.py` | The complete benchmark flow in readable order. |
| `day3/day3_postgres_to_csv_parquet_benchmark.py` | Compatibility wrapper so the original run command still works. |
| `day3/pipeline/constants.py` | Defaults and identifier validation patterns. |
| `day3/pipeline/config.py` | Environment variables and typed runtime config. |
| `day3/pipeline/logging_utils.py` | JSON logging setup. |
| `day3/pipeline/models.py` | Small result/artifact dataclasses. |
| `day3/pipeline/source.py` | PostgreSQL source query and DataFrame loading. |
| `day3/pipeline/exports.py` | CSV and Parquet writing. |
| `day3/pipeline/benchmark.py` | Repeated read timing for pandas and PyArrow. |
| `day3/pipeline/report.py` | JSON report and classroom summary table. |
| `day3/day3_agent_query_views.sql` | Optional SQL views for agent-style analysis. |

## Walkthrough Order For Class

### 1. Open `day3/lesson.py`

Show the pipeline in this order:

```text
load config
create database engine
load clean source data
prepare export frame
write CSV and Parquet
benchmark reads
build report
print summary
```

Do not open benchmark internals first.

### 2. Explain The Source

Open `day3/pipeline/source.py`.

Students only need to understand:

- the source is `training_data.clean_papers`
- rows are ordered deterministically
- JSONB columns are exported as text
- the result becomes a pandas DataFrame

The teaching point:

> Day 3 trusts Day 2's clean table and turns it into portable analytical files.

### 3. Explain Exports

Open `day3/pipeline/exports.py`.

Focus on:

- `frame.to_csv(...)`
- `pa.Table.from_pandas(...)`
- `pq.write_table(..., compression="snappy")`
- file size metadata

The teaching point:

> CSV is simple and readable; Parquet is columnar and optimized for analytics.

### 4. Explain Benchmarks

Open `day3/pipeline/benchmark.py`.

Focus on:

- pandas reads CSV
- pandas reads Parquet
- PyArrow reads CSV
- PyArrow reads Parquet
- each read is repeated several times

The teaching point:

> Benchmarks should measure the same data through comparable readers.

### 5. Explain The Report

Open `day3/pipeline/report.py`.

Focus on:

- source row count
- file sizes
- read timings
- speedup and size reduction summary

The teaching point:

> A benchmark is only useful if the result is captured and explainable.

### 6. Optional: Explain Agent Views

Open `day3/day3_agent_query_views.sql` only after the benchmark lesson.

The teaching point:

> Clean data can also be reshaped into stable SQL surfaces for agents or dashboards.

## What To Skip On First Pass

Skip these until learners ask:

- every field in the SELECT list
- every timing helper detail
- garbage collection before timing
- PyArrow internals
- materialized view internals
- every JSON report key

Those details are useful after students understand the format comparison story.

## Common Student Questions

### Why compare CSV and Parquet?

Because CSV is common and readable, while Parquet is often better for analytics and machine learning pipelines.

### Why use both pandas and PyArrow?

Because learners should see that performance depends on both file format and reader library.

### Why repeat reads?

One timing can be noisy. Repeated reads give a more stable comparison.

### Why write a JSON report?

Because benchmark results should be saved, not just printed once and forgotten.

### Why keep deterministic output names?

Because this lab is about comparison. Reusing the same names makes each run easy to find and inspect.

## Instructor Script

Use this short explanation:

> Day 3 starts with the clean papers from Day 2. We export the same rows to CSV and Parquet, read both files several times with pandas and PyArrow, then write a report showing size and speed differences.

That is the Day 3 lesson.
