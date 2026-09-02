# Day 2 Code Walkthrough

The slower version of the Day 2 README. Open `day2/lesson.py` first, and go into `day2/pipeline/` only when you want to see how one step works.

If you have not read the "Words You Will Need" section in [README.md](README.md), read that first. This file assumes you know what pagination, a watermark, and a child table are.

## The Shape Of The Day

Same four moves as Day 1, with two new ones wedged in:

```text
extract -> parse -> transform -> load -> remember where you stopped -> check
```

Parsing is new because the data arrives as XML rather than rows. Remembering is new because the source keeps growing, so the pipeline has to know where it got to.

```text
ArXiv API -> XML parser -> clean and rejected split -> PostgreSQL -> watermark
```

The idea to hold on to:

> A pipeline reading a live source has to handle four things a file never makes you think about: getting results a page at a time, turning nested data into flat rows, loading in a way that survives being run twice, and remembering where it stopped.

## What Changed Since Day 1

Day 1 read a static CSV. Everything new in Day 2 comes from the source being alive:

| Day 1 | Day 2 | Why it matters |
|---|---|---|
| One file, all at once | 100 papers per request | You need a loop, and you need to be polite about it |
| Already a table | Nested XML | Something has to walk the structure and flatten it |
| One row per record | Many authors per paper | Repeated values need a second table |
| Same file every time | Source keeps growing | You need to remember where you stopped |
| Run it again, same data | Pages overlap on purpose | Upserts stop the overlap becoming duplicates |

The pipeline is more complicated, but the shape underneath is the one you already know.

## Where Everything Lives

| File | What It Does |
|---|---|
| `day2/lesson.py` | The whole pipeline in readable order. |
| `day2/day2_arxiv_api_to_postgres.py` | A second way to run the same pipeline. |
| `day2/pipeline/constants.py` | Schema and table names, XML namespaces, default categories. |
| `day2/pipeline/config.py` | Reads `.env` and checks the settings. |
| `day2/pipeline/logging_utils.py` | Sets up the JSON log output. |
| `day2/pipeline/extract.py` | Talks to the ArXiv API, handles retries and paging. |
| `day2/pipeline/parse.py` | Turns XML entries into plain Python records. |
| `day2/pipeline/transform.py` | Splits records into clean and rejected. |
| `day2/pipeline/load.py` | Creates the tables and upserts the rows. |
| `day2/pipeline/state.py` | Reads and writes the watermark. |
| `day2/pipeline/outputs.py` | Writes the rejected sample CSV. |
| `day2/pipeline/checks.py` | Runs the checks after loading. |

## Going Through It In Class

### 1. Start with `day2/lesson.py`

Read it end to end first. The order is the lesson:

```text
load config
create tables
work out which time window to fetch
build the ArXiv query
fetch pages
split clean and rejected entries
load raw, clean, authors, and rejected rows
update the watermark
export the rejected sample
run checks
```

Notice that updating the watermark happens after loading, not before. If loading fails, the watermark does not move, and the next run tries the same window again. Getting that order wrong is how you silently lose data.

### 2. Configuration

Open `day2/pipeline/config.py`.

The database settings look like Day 1. What is new is everything controlling how the pipeline talks to the API: how many papers to fetch, how big each page is, how long to wait between requests, how long to wait before giving up on a slow response, and how many times to retry.

All of it lives in `.env`, which means you can slow the pipeline down, speed it up, or point it at different categories without editing any code.

> A pipeline that talks to a live service should be tunable without touching the code.

### 3. Extract

Open `day2/pipeline/extract.py`.

Three functions to look at:

- `build_search_query` turns your list of categories into the query string ArXiv expects.
- `request_feed_page` makes one HTTP request, and retries with a growing delay if it fails.
- `fetch_feed_page` wraps that up as "get me one page".

The retry logic deserves a minute in class. Networks fail. A public API can be briefly busy. Code that assumes every request succeeds works fine on your laptop and falls over the first time it runs unattended. Retrying with a growing wait, rather than hammering immediately, is the normal answer.

> Extraction owns the network: request shape, retries, rate limits, and getting the response back.

### 4. Parse

Open `day2/pipeline/parse.py`. This part has no equivalent in Day 1.

The API hands back XML with entries nested inside a feed, and things like authors and categories repeated inside each entry. This module walks that structure and produces a plain Python dictionary per paper.

Look at:

- `normalize_arxiv_id`, which strips the version suffix so `2603.05743v2` and `2603.05743v1` are recognised as the same paper.
- `entry_element_to_record`, which pulls out the fields we care about.
- The author and category extraction, which handle the repeated elements.

> Parsing turns source-shaped data into ordinary Python the rest of the pipeline can work with. Nothing downstream should have to know the source was XML.

### 5. Transform

Open `day2/pipeline/transform.py`.

Shorter than Day 1's, because most of the messy work already happened in parsing. It rejects an entry when the paper id, title, summary, published date, primary category, or author list is missing.

These are not arbitrary. Day 5 turns `summary` into training examples, so a paper without one is worthless three days from now. Rejecting it here, with a reason, is much better than finding out later.

> Validation decides which records are safe enough to keep.

### 6. Load

Open `day2/pipeline/load.py`. Do not read the SQL line by line in class.

Four destinations:

- raw entries, as they arrived
- clean papers
- paper authors, one row per author
- rejected entries, with reasons

The authors table is the interesting one. One paper, many authors, so it cannot be one row. The child table has a paper key pointing back at the paper, and that is how a nested structure becomes something you can query. This is the same move you make any time source data repeats.

> Nested source data usually becomes more than one table.

### 7. State

Open `day2/pipeline/state.py`. This is the part that makes the pipeline repeatable.

- `resolve_ingestion_window` decides which time range to fetch. First run, it goes back the default number of days. After that, it starts from the stored watermark, minus a small overlap.
- `update_state_from_latest_entry` writes the new position after a successful load.

The overlap looks wasteful and is not. Papers do not always show up in perfect time order, so re-reading the last ten minutes means you do not miss a late arrival. The upserts make those repeated rows harmless.

> Anything that runs repeatedly against a growing source has to remember where it stopped.

### 8. Check

Open `day2/pipeline/checks.py`.

Counts the rows in each table, looks for nulls where there should not be any, and checks the author table has no duplicate keys.

That last one is a real trap. Load authors twice without a proper key and you get every author listed twice, and every count that depends on authors quietly becomes wrong.

> Prove the load worked before trusting anything downstream.

## What To Skip The First Time

Leave these until someone asks:

- the XML namespace details
- every line of the table definitions
- every branch of the retry logic
- every field kept in the raw payload
- every environment variable
- every field in the log output

They matter, but not before the overall shape makes sense.

## Questions Students Ask

### Why does Day 2 need a watermark and Day 1 did not?

Because Day 1's file does not change. ArXiv gets new papers every day, so without a watermark every run would start from the beginning and redo work it has already done.

### Why put authors in a separate table?

Because one paper has many authors. Cramming them into one column means you can never cleanly ask "how many papers has this person written".

### Why keep the raw entries?

So you can fix a parser bug and re-parse without going back to the API. Given the API takes 15 minutes to walk, that is worth the disk space.

### Why keep the rejected records?

Because they tell you whether the source is incomplete or your rules are too strict. You cannot tell those apart without looking.

### Why upserts again?

Because pages overlap between runs on purpose. Without upserts that overlap would become duplicate rows every single run.

## The Short Version

> Day 2 takes a live ArXiv feed and treats it like a production source. We ask for it a page at a time, parse the XML into plain records, normalise each paper, split the valid from the invalid, write papers and authors into separate tables, and store a watermark so tomorrow's run knows where to carry on from.
