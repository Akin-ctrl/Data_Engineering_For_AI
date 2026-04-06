# Day 1 Code Walkthrough (Beginner Friendly)

## Why this document exists

This guide explains the Day 1 script in plain language so students can:

- Understand what every section is doing.
- Understand why each section exists in a production-minded pipeline.
- Read the script confidently without guessing.

Script explained in this guide:

- day1/day1_hf_csv_to_postgres.py

## Big picture: what the script does

The script runs a full one-shot data pipeline:

1. Read configuration from environment variables.
2. Resolve and download a CSV dataset from Hugging Face.
3. Load CSV into pandas safely.
4. Clean and validate data defensively.
5. Split rows into clean rows and rejected rows.
6. Upsert raw rows, clean rows, and rejected rows into PostgreSQL.
7. Export a sample rejected CSV for quick review.
8. Run post-load quality checks and log results.

## Execution flow in simple words

When you run the file directly, Python starts at the bottom:

- If script is executed as main program, it calls run_pipeline().
- If any exception happens, it logs a structured error and re-raises.

This means failures are visible and not silently swallowed.

## 1) Module header and imports

What this block does:

- Declares script purpose with a module docstring.
- Imports all libraries needed for hashing, JSON logging, environment loading, HTTP downloads, pandas transforms, and SQL operations.

Why it matters:

- Imports reveal dependencies and responsibilities.
- Keeping imports explicit helps students map features to libraries.

Notable imports and why:

- hashlib: deterministic row hashing for deduplication and idempotency.
- json: structured payload and log serialization.
- logging: production-grade runtime observability.
- re: column normalization and URL parsing.
- dataclass: typed config object.
- uuid4: unique batch identifier per run.
- pandas: in-memory data processing.
- requests: downloading dataset and calling Hugging Face API.
- dotenv: reading local .env settings.
- sqlalchemy: DB connection and SQL execution with parameters.

## 2) Global constants

Constants defined:

- SCHEMA_NAME = de_ai
- RAW_TABLE = raw_reviews
- CLEAN_TABLE = clean_reviews
- REJECT_TABLE = rejected_reviews

Expected input schema:

- EXPECTED_COLUMNS list defines exact normalized columns required by this dataset.

Why this is important:

- Centralized constants avoid hard-coded strings all over the script.
- Expected columns make schema drift fail fast, which is safer than silently corrupting data.

## 3) JsonFormatter class

What it does:

- Formats every log entry as a JSON object.
- Adds timestamp, severity, message, module, function, and line number.
- Optionally appends extra context if provided.

Why this is production-friendly:

- JSON logs are easy to search, filter, and aggregate.
- Context field supports structured troubleshooting.

Critical detail:

- json.dumps(..., default=str) prevents crashes when log context includes non-JSON-native values (example: Decimal).

## 4) configure_logger function and LOGGER singleton

What it does:

- Creates logger named day1_pipeline.
- Sets level to INFO.
- Attaches one stream handler with JsonFormatter.
- Returns logger object.

Why this pattern:

- Prevents duplicate handlers when script imports happen repeatedly.
- Keeps logging behavior consistent everywhere in script.

## 5) PipelineConfig dataclass

What it does:

- Stores all required runtime inputs from .env:
  - hf_csv_url
  - pghost
  - pgport
  - pgdatabase
  - pguser
  - pgpassword
- Provides computed property sqlalchemy_url.

Why dataclass is used:

- Cleaner, typed, immutable config object.
- Better readability than passing around many loose variables.

## 6) load_config function

What it does:

- Calls load_dotenv() so .env values become available in process.
- Enforces required variable presence.
- Raises clear ValueError if any are missing.
- Returns PipelineConfig.

Why this is defensive:

- Fails early before expensive work starts.
- Gives clear setup feedback to students.

## 7) normalize_column_name function

What it does:

- Lowercases names.
- Replaces non-alphanumeric chars with underscore.
- Collapses repeated underscores.
- Trims leading and trailing underscores.

Example:

- Positive Feedback Count becomes positive_feedback_count.

Why this matters:

- CSV headers often contain spaces and punctuation.
- Normalized headers make downstream code stable and predictable.

## 8) stable_row_hash function

What it does:

- Converts each row to a normalized dictionary.
- Replaces missing values with empty strings.
- Sorts keys before JSON serialization.
- Hashes serialized content with SHA-256.

Why this matters:

- Same logical row always gets same hash.
- Enables idempotent upserts and traceability across runs.

## 9) find_first_existing_column function

Current status:

- Helper that finds first matching column from candidate names.
- Not used in current dataset-specific version.

Why keep it:

- Useful utility for future labs with varied schemas.
- Good teaching example of reusable helper functions.

## 10) Hugging Face URL helpers

Functions:

- parse_hf_dataset_id
- resolve_huggingface_csv_url

What they do:

- Accept either dataset page URL or direct CSV resolve URL.
- If resolve URL already provided, return it.
- Else parse dataset id from page URL.
- Call Hugging Face dataset API.
- Find CSV filename in repository siblings.
- Build a direct CSV download URL.

Why this is useful for students:

- Reduces setup friction.
- Makes script robust against different user input styles.

## 11) download_csv function

What it does:

- Creates destination directory if needed.
- Downloads bytes from URL with timeout.
- Raises on HTTP failure.
- Saves file locally and returns path.

Why this is defensive:

- Timeout prevents hanging forever.
- raise_for_status surfaces bad URLs or network errors immediately.

## 12) read_csv_defensively function

What it does:

- Reads CSV as strings first.
- Treats common null tokens as missing values.
- Skips malformed rows instead of crashing whole run.

Why this approach:

- Day 1 teaches resilience first.
- Type coercion is done later in controlled validation logic.

## 13) ensure_schema_and_tables function

What it does:

- Creates schema de_ai if missing.
- Creates three tables if missing:
  - raw_reviews
  - clean_reviews
  - rejected_reviews

Design rationale:

- raw_reviews stores traceable source records and metadata.
- clean_reviews stores validated, analytics-ready fields.
- rejected_reviews stores invalid records and reasons.

Key constraints:

- row_hash unique in raw and rejected for idempotent conflict handling.
- review_key primary key in clean for upsert conflict target.

## 14) clean_dataset_defensively function

This is the most important teaching function.

Step-by-step:

1. Copy input dataframe and normalize column names.
2. Add row_number and deterministic row_hash.
3. Enforce expected schema presence.
4. Coerce numeric columns:
   - review_id from unnamed_0
   - clothing_id
   - age
   - rating
   - recommended_ind
   - positive_feedback_count
5. Trim text columns.
6. Build review_key:
   - Use review_id when available.
   - Fallback to row_hash if review_id missing.
7. Validate each row and collect rejection reasons:
   - missing or invalid clothing_id
   - missing or invalid rating
   - rating out of 1..5 range
   - missing or invalid recommended_ind
   - recommended_ind not binary 0 or 1
   - missing or invalid positive_feedback_count
   - negative positive_feedback_count
   - missing review_text
   - age outside 13..100 (if present)
8. Join row reasons into reject_reason string.
9. Build raw_payload safely:
   - normalize payload columns
   - convert NaN to None to keep JSON valid
10. Split into clean and rejected dataframes.
11. Return only the selected output columns for each set.

Why this is excellent for Day 1:

- Shows practical quality gates.
- Demonstrates how to keep bad data without losing it.
- Introduces observability and auditability concepts early.

## 15) upsert_raw_records function

What it does:

- Converts each normalized raw row into parameter payload.
- Inserts into raw_reviews.
- On row_hash conflict, updates existing row.

Why this matters:

- Supports reruns without duplicate explosion.
- Preserves latest ingestion metadata.

## 16) upsert_clean_records function

What it does:

- Converts clean dataframe into typed DB parameters.
- Normalizes nullable text fields to None.
- Inserts into clean_reviews.
- On review_key conflict, updates all mutable fields.

Why this is important:

- Demonstrates robust upsert logic for structured curated data.
- Keeps data current across reruns.

## 17) upsert_rejected_records function

What it does:

- Writes rejected records with reason and raw_payload.
- Upserts on row_hash conflict.

Why this matters:

- Rejections are not lost.
- Learners can inspect failures and improve rules.

## 18) export_rejected_csv_sample function

What it does:

- Writes top 100 rejected rows to output CSV.
- Serializes raw_payload as JSON string for readability.

Why this helps classwork:

- Fast manual inspection without querying DB.
- Easy to discuss common error patterns in class.

## 19) run_post_load_checks function

What it checks:

- raw_count for this batch.
- clean_count for this batch.
- reject_count for this batch.
- clean_null_violations in critical fields.
- duplicate review keys in clean table.
- sample rating distribution.

Why this is key:

- Confirms pipeline correctness immediately.
- Gives measurable evidence after each run.

## 20) run_pipeline function (orchestration)

This is the central workflow controller.

Execution order:

1. Load config.
2. Create unique batch_id.
3. Prepare output paths.
4. Log start event.
5. Resolve source URL and download CSV.
6. Read CSV into pandas dataframe.
7. Fail if dataframe is empty.
8. Create database engine.
9. Ensure schema/tables exist.
10. Build normalized raw dataframe with row_number and row_hash.
11. Upsert raw records.
12. Clean and split into clean/rejected.
13. Upsert clean records.
14. Upsert rejected records.
15. Export rejected CSV sample.
16. Run post-load checks.
17. Log successful completion with row counts.

Why this style is good teaching design:

- Sequence is explicit and easy to narrate.
- Each step is delegated to a focused helper function.
- Students can test one stage at a time.

## 21) Entry point and exception handling

The final block:

- Calls run_pipeline inside try.
- On exception, logs structured error with type and message.
- Re-raises to preserve non-zero exit code.

Why this is best practice:

- You get clear logs.
- Your shell and schedulers still detect failure.

## How to explain this script to beginners in class

Use this 5-part narrative:

1. Configuration: Where does the script get settings?
2. Ingestion: How does data arrive locally?
3. Validation: How do we decide what is good vs bad?
4. Persistence: Where do raw, clean, and rejected records go?
5. Verification: How do we know the run succeeded correctly?

## Common student questions and answers

Q: Why keep both raw and clean tables?

A: Raw is audit trail and recovery source. Clean is trusted working data.

Q: Why reject rows instead of auto-fixing everything?

A: Silent auto-fixes can hide bad assumptions. Rejecting with reasons is safer.

Q: Why upsert instead of insert only?

A: Upsert allows safe reruns and idempotent behavior.

Q: Why hash rows?

A: Hashes give stable identity when natural IDs are missing or dirty.

Q: Why JSON logs?

A: Structured logs are easier to parse in production tooling.

## Suggested Day 1 teaching exercise

Ask students to modify one rule safely:

- Example: Add rule that review_text must be at least 15 characters.

Then ask them to:

1. Implement rule.
2. Rerun pipeline.
3. Compare clean and rejected counts.
4. Inspect rejected sample file and explain impact.

This reinforces defensive engineering and measurable data quality outcomes.
