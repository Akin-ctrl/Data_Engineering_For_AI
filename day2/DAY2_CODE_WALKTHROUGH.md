# Day 2 Code Walkthrough

This walkthrough explains the Day 2 pipeline at a teaching pace. The goal is to show how a live API feed becomes a clean, repeatable, database-backed training corpus without hiding the important implementation details.

## 0. Imports and Libraries

The script imports the following modules and each one has a specific job:

- `csv`: parses and writes CSV-style data used in helper routines.
- `hashlib`: creates stable SHA256 hashes so rows can be deduplicated safely.
- `json`: serializes raw payloads and structured logs.
- `logging`: writes the structured runtime logs.
- `os`: reads environment variables.
- `re`: validates and normalizes query/text patterns.
- `time`: provides sleep-based rate limiting and backoff.
- `xml.etree.ElementTree as ET`: parses the ArXiv Atom/XML feed.
- `dataclasses.dataclass`: defines small typed configuration objects.
- `datetime`, `timedelta`, `timezone`: handle timestamps and watermark logic.
- `pathlib.Path`: handles file paths in a readable, cross-platform way.
- `typing.Any`: keeps nested payload annotations explicit.
- `uuid.uuid4`: creates batch identifiers.
- `requests`: performs HTTP requests to the ArXiv API.
- `dotenv.load_dotenv`: loads local environment variables from `.env`.
- `requests.Response`: documents the response object type.
- `requests.exceptions.RequestException`: catches network and transport failures.
- `sqlalchemy.create_engine` and `sqlalchemy.text`: connect to PostgreSQL and run SQL.
- `sqlalchemy.engine.Engine`: annotates the database engine type.

The supporting pattern here is simple: standard library for parsing and control flow, `requests` for HTTP, and SQLAlchemy for PostgreSQL.

## 1. Why Day 2 Exists

Day 1 used a static CSV. Day 2 adds a live API, pagination, nested records, retries, and a small persistence layer for sync state. That is a more realistic ingestion pattern and it is closer to what students will see in production.

## 2. Why ArXiv Works Well for Teaching

ArXiv is public, stable, and easy to query. The feed returns paper metadata, repeated authors, categories, and links. That gives us enough structure to practice flattening without needing a complex auth setup. It also supports larger pulls, which helps us build a corpus big enough for baseline deep learning experiments.

## 3. The Real Payload Shape

The API returns Atom/XML, not JSON. Each entry includes paper metadata, repeated `<author>` nodes, category tags, and link elements. The script first converts each entry into a nested Python dictionary and only then flattens it.

## 4. Configuration

The script reads environment variables for PostgreSQL and ArXiv settings. This keeps secrets out of the code and makes the lab easy to change in class.

Important settings for the larger Day 2 run:

- `ARXIV_CATEGORIES` drives the multi-category crawl.
- `ARXIV_SEARCH_QUERY` may be left blank; the script builds the query from `ARXIV_CATEGORIES`.
- `ARXIV_MAX_PAPERS=10000` makes the lab large enough for a meaningful training corpus.
- `ARXIV_DEFAULT_LOOKBACK_DAYS` controls how far back the first incremental run starts.
- `ARXIV_REQUEST_TIMEOUT_SECONDS`, `ARXIV_REQUEST_MAX_RETRIES`, `ARXIV_RETRY_BACKOFF_SECONDS`, and `ARXIV_USER_AGENT` keep the public API pull polite and resilient.

The `load_config()` function also validates PostgreSQL settings, so the script fails early if a required secret or connection value is missing.

## 5. Logging

The script uses structured JSON logging so the console output stays machine-readable and easy to debug. This matters because the pipeline needs to explain network retries, pagination progress, and load results without exposing secrets.

The logger is configured by `configure_logger()`, and the custom `JsonFormatter` class formats each record as JSON with timestamp, level, message, module, function, and line number.

## 6. Pagination

The pipeline requests the feed page by page, using `start` and `max_results`. It sleeps between requests so the class can talk about rate limiting and respectful API use.

The paginator stops when one of these happens:

- the requested paper limit is reached,
- the feed returns fewer rows than requested,
- or repeated API failures exhaust retries.

That makes the loop easy to reason about and safe to rerun.

The pagination behavior is implemented in the API request helpers and the main pipeline loop, which use `requests`, `time.sleep`, and the retry counters from configuration.

## 7. Watermark State

Instead of storing pagination state in memory, the script keeps a small watermark table in PostgreSQL. The watermark records the latest published timestamp and the last paper key seen.

This is the simplest durable option for a class demo because it avoids external state files and still supports incremental runs.

The watermark is managed through the `pipeline_state` table and the `get_state()` / `upsert_state()` functions.

## 8. Why This Is the Best Balance

This approach is simple enough to explain, but durable enough for repeated runs. It avoids duplicate inserts while still showing a real production pattern.

It is a better teaching choice than a pure in-memory cursor because it shows where state should live when the pipeline is restarted.

## 9. Parsing the Feed

Each entry is parsed into a nested dictionary containing the paper id, title, summary, timestamps, categories, author list, links, and the original payload.

The parser also creates a stable row hash. That helps the script identify the same paper across repeated runs even when the raw feed is re-read.

Parsing is handled by helper functions that use `xml.etree.ElementTree`, namespace maps, and the `stable_hash()` function.

## 10. Normalizing the Paper Record

The clean papers table keeps the core analytical fields:

- paper id
- version
- title
- summary
- published and updated timestamps
- primary category
- all categories as JSON
- author summary fields
- ArXiv and PDF links

This is the table students would normally query first when training a model or doing exploration.

The fields written here are built in the flattening and upsert functions, especially `upsert_clean_records()` and the parsing helpers that prepare the rows.

## 11. Normalizing Authors

Repeated authors are written to `paper_authors` as one row per author per paper. That is the normalized part of the model.

The relationship is one paper to many authors. This is why the author table uses `(paper_key, author_order)` as its conflict key.

The author table is written by `upsert_author_records()`, which expands the nested author list into a single row per author.

## 12. Why There Is No Affiliation Table

The live feed does not reliably provide affiliations. Since the data does not support it, the script does not invent it.

That is a deliberate teaching point: schema design should follow the source data, not assumptions.

## 13. Rejected Records

Malformed or incomplete entries are written to a rejected table with the raw payload and a reason string.

In the current 10k+ configuration, the rejection table is usually empty, but the path still matters because it shows how to preserve failures for review.

Rejected rows are produced by `split_clean_and_reject()` and written by `upsert_rejected_records()`.

## 14. Raw Records

The raw table stores the parsed nested payload. This preserves traceability if a student wants to inspect what was received from the API.

Raw records are also useful for replay and debugging if the flattening logic changes later.

Raw rows are written by `upsert_raw_records()`, which preserves the parsed payload for auditability.

## 15. Idempotency

All target tables use conflict keys so repeated runs do not create duplicates. The paper table upserts by normalized paper key, and the author table upserts by paper key plus author order.

The state table stores the watermark in PostgreSQL, so repeated executions can resume from the last successful pull.

The conflict-handling logic lives in the SQL inside the upsert functions and is what makes the Day 2 load safe to rerun.

## 16. Post-Load Checks

The script checks row counts, rejected counts, and a few quality conditions after load. That gives learners immediate feedback that the pipeline worked.

The checks are intentionally simple: counts, null checks, and duplicate-key checks are enough for a classroom demo and easy for beginners to understand.

Those checks are grouped in `run_post_load_checks()` so the validation remains separate from the ingestion and loading steps.

## 17. Why This Can Train a Model

This dataset is suitable for baseline machine learning and starter deep learning because it has enough rows, rich summaries, authors, and multiple categories.

Best first model targets:

- classify `primary_category` from `title + summary`,
- train semantic embeddings for retrieval,
- or build a multi-label classifier if you expand the category set further.

## 18. Teaching Point

The important lesson is not just how to call an API. It is how to turn a live, slightly messy feed into stable analytical tables that survive repeated execution.

## 19. Function Map

This is the full function map in the script so learners can trace the code from top to bottom:

- `configure_logger()`: creates the JSON logger.
- `parse_csv_list()`: turns comma-separated environment values into a list.
- `parse_positive_int()`: validates integer configuration values.
- `load_config()`: loads and validates environment settings.
- `stable_hash()`: generates deterministic row hashes.
- `strip_text()`: normalizes text fields.
- `parse_iso_timestamp()`: parses ArXiv timestamps.
- `build_search_query()`: constructs the ArXiv search string.
- `request_feed_page()`: fetches one API page with retries.
- `split_clean_and_reject()`: separates valid rows from invalid rows.
- `upsert_raw_records()`: saves raw payloads.
- `upsert_clean_records()`: saves flattened paper rows.
- `upsert_author_records()`: saves one row per author.
- `upsert_rejected_records()`: saves rejected rows and reasons.
- `get_state()`: reads the persisted watermark.
- `upsert_state()`: writes the updated watermark.
- `run_post_load_checks()`: validates the load after inserts.
- `run_pipeline()`: runs the full ingestion flow.

## 20. Class Map

The Day 2 script is mostly functional, but the `PipelineConfig` dataclass is the one class that matters here.

- `PipelineConfig`: groups all ArXiv and PostgreSQL settings into one typed object so the rest of the script can pass around one clear config instead of many loose variables.