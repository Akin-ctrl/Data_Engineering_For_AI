# Day 1 Code Walkthrough

Day 1 has been refactored so the first file you open in class is small and readable.

Start here:

- `day1/lesson.py`

Use the files inside `day1/pipeline/` only when you want to explain one specific implementation detail.

## Big Picture

Day 1 teaches one complete data engineering pattern:

```text
CSV source -> read with pandas -> clean/reject split -> PostgreSQL -> quality checks
```

The important learning idea is:

> Clean data and rejected data should both be stored deliberately.

Bad rows should not silently disappear, and they should not pollute the clean table.

## The Teaching Entry Point

`day1/lesson.py` is the classroom entrypoint.

The main function is intentionally short:

```text
load config
download CSV
read CSV
prepare raw rows
clean and reject rows
create tables
load raw, clean, and rejected rows
export rejected sample
run checks
```

That is the whole Day 1 story.

## File Map

| File | What It Explains |
|---|---|
| `day1/lesson.py` | The complete pipeline flow in readable order. |
| `day1/day1_hf_csv_to_postgres.py` | Compatibility wrapper so the original run command still works. |
| `day1/pipeline/config.py` | Environment variables and typed runtime config. |
| `day1/pipeline/logging_utils.py` | JSON logging setup. |
| `day1/pipeline/extract.py` | Hugging Face URL resolution, download, and CSV reading. |
| `day1/pipeline/transform.py` | Column cleanup, row hashing, validation, and rejected-row logic. |
| `day1/pipeline/load.py` | PostgreSQL schema creation and upserts. |
| `day1/pipeline/outputs.py` | Rejected sample CSV export. |
| `day1/pipeline/checks.py` | Post-load quality checks. |

## Walkthrough Order For Class

### 1. Open `day1/lesson.py`

Show learners that a pipeline is an ordered story:

```text
extract -> transform -> load -> check
```

Do not open the helper modules yet.

### 2. Explain Configuration

Open `day1/pipeline/config.py`.

Students only need to understand:

- values come from `.env`
- required values are checked early
- the database connection string is built from config

The teaching point:

> Runtime settings should not be hard-coded inside pipeline logic.

### 3. Explain Extraction

Open `day1/pipeline/extract.py`.

Students only need to understand:

- the Hugging Face URL is resolved
- the CSV is downloaded into `day1/output/`
- pandas reads the file defensively

The teaching point:

> Extract code gets data into memory; it should not clean or load it.

### 4. Explain Transformation

Open `day1/pipeline/transform.py`.

Focus on:

- `normalize_column_name`
- `stable_row_hash`
- `prepare_raw_records_for_load`
- `clean_dataset_defensively`

The teaching point:

> Transformation is where we decide what is acceptable, what is rejected, and why.

### 5. Explain Loading

Open `day1/pipeline/load.py`.

Do not explain every SQL column line-by-line.

Focus on the three destinations:

- raw table
- clean table
- rejected table

The teaching point:

> A good pipeline preserves traceability: original rows, accepted rows, and rejected rows.

### 6. Explain Checks

Open `day1/pipeline/checks.py`.

Focus on:

- row counts
- null checks
- duplicate checks
- rating distribution sample

The teaching point:

> A pipeline is not finished when it writes data. It is finished when it proves the write worked.

## What To Skip On First Pass

Skip these until learners ask:

- detailed SQL DDL
- logger internals
- exact JSON log shape
- every field in `PipelineConfig`
- every branch in the reject logic

Those details matter, but they are second-pass material.

## Common Student Questions

### Why keep raw rows?

Because clean data alone cannot explain what happened. Raw rows let us audit, replay, and debug.

### Why keep rejected rows?

Because rejected rows are evidence. They tell us whether the source data is broken, the validation rules are too strict, or the pipeline needs improvement.

### Why use row hashes?

Hashes give us stable row identities. That helps with deduplication and repeatable loads.

### Why use upserts?

Upserts let the script run more than once without blindly duplicating data.

### Why split the code into modules?

Because each module now has one teaching responsibility:

```text
config, extract, transform, load, output, check
```

That makes the repo easier to explain and easier to debug.

## Instructor Script

Use this short explanation:

> Day 1 takes a public CSV and treats it like production data. We do not trust it blindly. We download it, read it carefully, preserve the raw version, validate each row, send good rows to a clean table, send bad rows to a rejected table, and then run checks to prove the load worked.

That is the Day 1 lesson.
