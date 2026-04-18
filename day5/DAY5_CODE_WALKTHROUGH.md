# Day 5 Code Walkthrough

This walkthrough explains how Day 5 converts the clean text corpus from earlier labs into strict instruction-tuning payloads ready for model training.

## 0. Imports and Libraries

The Day 5 script uses these imports and each one has a specific role:

- `hashlib`: creates deterministic IDs from chunk content.
- `json`: serializes JSONL payload rows and manifest output.
- `logging`: writes structured runtime logs.
- `os`: reads environment variables.
- `random`: performs deterministic train/validation splitting with a fixed seed.
- `re`: validates SQL identifiers and performs sentence splitting.
- `dataclasses.dataclass`: defines typed configuration and record containers.
- `datetime`, `timezone`: timestamps logs and manifest metadata.
- `pathlib.Path`: handles output paths cleanly.
- `statistics.mean`: computes summary metrics used in validation logs.
- `typing.Any`: keeps metadata payload typing explicit.
- `pandas`: reads SQL results into a DataFrame.
- `dotenv.load_dotenv`: loads `.env` variables.
- `sqlalchemy.create_engine`, `sqlalchemy.text`: connects to PostgreSQL and runs the source query.

The architecture mirrors previous days: typed config, structured logs, and explicit helper functions.

## 1. Why Day 5 Exists

Days 1 to 4 produce clean and queryable data assets. Day 5 adds the final training-data transformation layer: converting text rows into supervised instruction pairs for fine-tuning.

## 2. Constants and Defaults

The script defines all key defaults in one place:

- source table and ordering defaults
- output directory and file basename
- chunk size and overlap
- validation minimums
- split ratio and split seed

This keeps behavior predictable and makes classroom tuning easy via `.env` values.

## 3. Logger Setup

`JsonFormatter` and `configure_logger()` are reused patterns from earlier days:

- Every log line is JSON.
- Context fields are optional and structured.
- Logs are beginner-readable but production-shaped.

## 4. Day5Config Dataclass

`Day5Config` contains all runtime values and all output-path properties.

Important properties:

- `sqlalchemy_url`
- `alpaca_all_path`, `alpaca_train_path`, `alpaca_val_path`
- `chat_all_path`, `chat_train_path`, `chat_val_path`
- `manifest_path`

Keeping these as computed properties prevents path-string duplication across functions.

## 5. Record Dataclasses

The script defines three record types:

- `ChunkRecord`: one chunk produced from one paper.
- `AlpacaRecord`: strict instruction/input/output payload row.
- `ChatRecord`: strict messages payload row.

These dataclasses make the transformation stages explicit and testable.

## 6. Config Validation Helpers

The parser helpers (`parse_positive_int`, `parse_non_negative_int`, `parse_ratio`, `validate_identifier`) enforce safe settings:

- numeric fields cannot be malformed
- overlap cannot exceed chunk size
- table/order identifiers are validated before SQL interpolation

This is defensive coding that prevents hidden runtime surprises.

## 7. Loading Config

`load_config()` reads `.env`, validates required PostgreSQL values, and loads Day 5 options.

It also resolves output paths relative to the repository root, matching earlier day patterns.

## 8. Text Normalization and Sentence Splitting

Core text helpers:

- `normalize_whitespace()`: collapses repeated spaces/newlines.
- `split_sentences()`: sentence-like split using regex boundaries.
- `word_count()`: simple token count for validation and chunking logic.
- `tail_words()`: overlap bridge between consecutive chunks.

These functions provide deterministic preprocessing without introducing heavy NLP dependencies.

## 9. Chunking Algorithm

The chunking path combines three helpers:

- `split_long_sentence()`: window-splits oversized sentences.
- `chunk_text()`: packs sentences up to max words and applies overlap.
- `build_chunk_records()`: converts source rows into typed chunk records.

Behavior details:

- chunk strategy is sentence-first
- overlap is word-based tail carryover
- each chunk gets a stable hashed `chunk_id`

This matches your chosen strategy: sentence-based chunks with overlap and capped chunk size.

## 10. Source Query

`build_source_sql()` selects:

- `paper_key`
- `title`
- `summary`
- `primary_category`
- `published_at`

`load_source_frame()` executes the SQL via SQLAlchemy and returns a DataFrame.

If no rows are returned, the script fails fast.

## 11. Instruction Task Templates

`build_instruction_triples()` generates three tasks per chunk:

- `summarize`
- `classify`
- `keypoints`

Output generation is deterministic:

- summary uses leading extractive sentences
- classification returns the row primary category
- keypoints uses top sentence bullets

This gives consistent labels without requiring an online model call.

## 12. Format Builders

`build_alpaca_records()` creates strict Alpaca rows with:

- `instruction`
- `input`
- `output`
- metadata block

`build_chat_records()` maps each Alpaca row to a strict two-message chat format:

- one `user` message
- one `assistant` message

This fulfills the dual-export requirement in one run.

## 13. Strict Validation

Validation functions:

- `validate_alpaca_records()`
- `validate_chat_records()`

Checks include:

- required content presence
- minimum input/output word counts
- task-specific output minimums (classification accepts a single category token)
- unique record IDs
- strict chat role structure

The script stops on first violation so bad payloads are never exported silently.

## 14. Deterministic Split

`split_ids()` performs seeded random splitting:

- default ratio: 90/10
- default seed: 42
- ensures neither split is empty

Since IDs are stable, reruns with same source and seed produce the same split.

## 15. Output Writers

`write_jsonl()` writes line-delimited JSON records for both formats and both splits.

`build_manifest()` writes run metadata including:

- source settings
- chunking settings
- task counts
- split statistics
- file paths
- validation settings

The manifest is the audit summary for downstream training steps.

## 16. Pipeline Orchestration

`run_pipeline()` is the Day 5 controller:

1. load config
2. load source frame
3. build chunks
4. build Alpaca + chat records
5. validate records
6. split train/val deterministically
7. write all JSONL files
8. write manifest
9. log completion metrics

This mirrors the explicit, beginner-first orchestration style used in prior days.

## 17. Function Map

Full function map for the Day 5 script:

- `configure_logger()`
- `validate_identifier()`
- `parse_positive_int()`
- `parse_non_negative_int()`
- `parse_ratio()`
- `resolve_relative_path()`
- `load_config()`
- `normalize_whitespace()`
- `split_sentences()`
- `word_count()`
- `tail_words()`
- `split_long_sentence()`
- `chunk_text()`
- `build_source_sql()`
- `load_source_frame()`
- `build_chunk_records()`
- `extractive_summary()`
- `keypoints_output()`
- `build_instruction_triples()`
- `build_alpaca_records()`
- `build_chat_records()`
- `validate_alpaca_records()`
- `validate_chat_records()`
- `split_ids()`
- `write_jsonl()`
- `build_manifest()`
- `run_pipeline()`

## 18. Class Map

Classes used in Day 5:

- `Day5Config`: runtime configuration and canonical output paths.
- `ChunkRecord`: one chunk unit tied to source paper metadata.
- `AlpacaRecord`: strict instruction-tuning tuple in Alpaca layout.
- `ChatRecord`: strict instruction-tuning tuple in chat layout.

## 19. Teaching Point

The key lesson is that turning clean tables into fine-tuning datasets is an explicit engineering step. You need chunking policy, schema policy, and validation policy before you have a trustworthy AI payload.
