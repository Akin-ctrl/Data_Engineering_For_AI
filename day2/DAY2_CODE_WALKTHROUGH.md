# Day 2 Code Walkthrough

This walkthrough explains the Day 2 pipeline at a teaching pace. The goal is to show how a live API feed becomes a clean, repeatable, database-backed training corpus without hiding the important implementation details.

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

## 5. Logging

The script uses structured JSON logging so the console output stays machine-readable and easy to debug. This matters because the pipeline needs to explain network retries, pagination progress, and load results without exposing secrets.

## 6. Pagination

The pipeline requests the feed page by page, using `start` and `max_results`. It sleeps between requests so the class can talk about rate limiting and respectful API use.

The paginator stops when one of these happens:

- the requested paper limit is reached,
- the feed returns fewer rows than requested,
- or repeated API failures exhaust retries.

That makes the loop easy to reason about and safe to rerun.

## 7. Watermark State

Instead of storing pagination state in memory, the script keeps a small watermark table in PostgreSQL. The watermark records the latest published timestamp and the last paper key seen.

This is the simplest durable option for a class demo because it avoids external state files and still supports incremental runs.

## 8. Why This Is the Best Balance

This approach is simple enough to explain, but durable enough for repeated runs. It avoids duplicate inserts while still showing a real production pattern.

It is a better teaching choice than a pure in-memory cursor because it shows where state should live when the pipeline is restarted.

## 9. Parsing the Feed

Each entry is parsed into a nested dictionary containing the paper id, title, summary, timestamps, categories, author list, links, and the original payload.

The parser also creates a stable row hash. That helps the script identify the same paper across repeated runs even when the raw feed is re-read.

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

## 11. Normalizing Authors

Repeated authors are written to `paper_authors` as one row per author per paper. That is the normalized part of the model.

The relationship is one paper to many authors. This is why the author table uses `(paper_key, author_order)` as its conflict key.

## 12. Why There Is No Affiliation Table

The live feed does not reliably provide affiliations. Since the data does not support it, the script does not invent it.

That is a deliberate teaching point: schema design should follow the source data, not assumptions.

## 13. Rejected Records

Malformed or incomplete entries are written to a rejected table with the raw payload and a reason string.

In the current 10k+ configuration, the rejection table is usually empty, but the path still matters because it shows how to preserve failures for review.

## 14. Raw Records

The raw table stores the parsed nested payload. This preserves traceability if a student wants to inspect what was received from the API.

Raw records are also useful for replay and debugging if the flattening logic changes later.

## 15. Idempotency

All target tables use conflict keys so repeated runs do not create duplicates. The paper table upserts by normalized paper key, and the author table upserts by paper key plus author order.

The state table stores the watermark in PostgreSQL, so repeated executions can resume from the last successful pull.

## 16. Post-Load Checks

The script checks row counts, rejected counts, and a few quality conditions after load. That gives learners immediate feedback that the pipeline worked.

The checks are intentionally simple: counts, null checks, and duplicate-key checks are enough for a classroom demo and easy for beginners to understand.

## 17. Why This Can Train a Model

This dataset is suitable for baseline machine learning and starter deep learning because it has enough rows, rich summaries, authors, and multiple categories.

Best first model targets:

- classify `primary_category` from `title + summary`,
- train semantic embeddings for retrieval,
- or build a multi-label classifier if you expand the category set further.

## 18. Teaching Point

The important lesson is not just how to call an API. It is how to turn a live, slightly messy feed into stable analytical tables that survive repeated execution.