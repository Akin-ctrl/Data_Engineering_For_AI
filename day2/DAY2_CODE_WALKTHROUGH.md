# Day 2 Code Walkthrough

Day 2 has been refactored so the first file you open in class is small and readable.

Start here:

- `day2/lesson.py`

Use the files inside `day2/pipeline/` only when you want to explain one specific implementation detail.

## Big Picture

Day 2 teaches how a live API feed becomes clean, repeatable, database-backed training data.

```text
ArXiv API -> Atom/XML parser -> clean/reject split -> PostgreSQL -> watermark
```

The important learning idea is:

> Live sources need pagination, validation, idempotent loading, and state.

## What Changes From Day 1

Day 1 used a static CSV.

Day 2 adds:

- a live API
- XML parsing
- pagination
- repeated authors
- a child table
- retry/backoff behavior
- a database watermark

That is why Day 2 is more complex, but the pipeline shape is still familiar:

```text
configure -> extract -> parse -> transform -> load -> check
```

## File Map

| File | What It Explains |
|---|---|
| `day2/lesson.py` | The complete pipeline flow in readable order. |
| `day2/day2_arxiv_api_to_postgres.py` | Compatibility wrapper so the original run command still works. |
| `day2/pipeline/constants.py` | Schema names, table names, XML namespaces, and default categories. |
| `day2/pipeline/config.py` | Environment variables and typed runtime config. |
| `day2/pipeline/logging_utils.py` | JSON logging setup. |
| `day2/pipeline/extract.py` | ArXiv API requests, retries, and page fetching. |
| `day2/pipeline/parse.py` | Atom/XML entry parsing and paper-record normalization. |
| `day2/pipeline/transform.py` | Clean/rejected record splitting. |
| `day2/pipeline/load.py` | PostgreSQL schema creation and upserts. |
| `day2/pipeline/state.py` | Watermark read/write logic. |
| `day2/pipeline/outputs.py` | Rejected sample CSV export. |
| `day2/pipeline/checks.py` | Post-load quality checks. |

## Walkthrough Order For Class

### 1. Open `day2/lesson.py`

Show the pipeline in this order:

```text
load config
create tables
resolve ingestion window
build ArXiv query
fetch pages
split clean and rejected entries
load raw, clean, authors, and rejected rows
update watermark
export rejected sample
run checks
```

Do not open every helper module yet.

### 2. Explain Configuration

Open `day2/pipeline/config.py`.

Students only need to understand:

- ArXiv settings come from `.env`
- PostgreSQL settings come from `.env`
- categories can build the search query automatically
- request timeout/retry settings make live API calls safer

The teaching point:

> Live API pipelines should be configurable without changing code.

### 3. Explain Extraction

Open `day2/pipeline/extract.py`.

Focus on:

- `build_search_query`
- `request_feed_page`
- `fetch_feed_page`

The teaching point:

> Extraction handles network behavior: request parameters, retries, rate limits, and response parsing.

### 4. Explain Parsing

Open `day2/pipeline/parse.py`.

Focus on:

- `normalize_arxiv_id`
- `entry_element_to_record`
- author extraction
- category extraction

The teaching point:

> Parsing converts source-specific XML into normal Python records the rest of the pipeline can understand.

### 5. Explain Transformation

Open `day2/pipeline/transform.py`.

Focus on:

- missing paper ids
- missing title/summary
- missing timestamps
- missing primary category
- missing authors

The teaching point:

> Validation decides which records are safe enough for the clean table.

### 6. Explain Loading

Open `day2/pipeline/load.py`.

Do not explain every SQL column line-by-line.

Focus on the four destinations:

- raw ArXiv entries
- clean papers
- paper authors
- rejected entries

The teaching point:

> Nested source data often becomes multiple relational tables.

### 7. Explain State

Open `day2/pipeline/state.py`.

Focus on:

- `resolve_ingestion_window`
- `update_state_from_latest_entry`

The teaching point:

> A pipeline that runs repeatedly needs to remember where it stopped.

### 8. Explain Checks

Open `day2/pipeline/checks.py`.

Focus on:

- raw row count
- clean row count
- author row count
- rejected row count
- null violations
- duplicate author keys

The teaching point:

> The pipeline should prove that the load worked before we trust the output.

## What To Skip On First Pass

Skip these until learners ask:

- full XML namespace details
- every SQL DDL line
- every retry/backoff branch
- every field in the raw payload
- every environment variable
- every logging context field

Those details are useful after students understand the main pipeline shape.

## Common Student Questions

### Why does Day 2 need a watermark?

Because the source is live. If the pipeline runs tomorrow, it should know where the previous run ended.

### Why store authors separately?

Because one paper can have many authors. A child table models that repeated relationship cleanly.

### Why keep raw entries?

Because raw entries let us audit, replay, and debug the parser later.

### Why still keep rejected records?

Because rejected records show whether the source is incomplete or whether our validation rules are too strict.

### Why use upserts?

Because API pages can overlap across runs. Upserts make repeated ingestion safer.

## Instructor Script

Use this short explanation:

> Day 2 takes a live ArXiv feed and treats it like a production source. We query it in pages, parse the XML, normalize each paper, split valid and invalid records, write papers and authors into PostgreSQL, and store a watermark so the next run knows where to continue.

That is the Day 2 lesson.
