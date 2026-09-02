# Day 2 Lab: ArXiv API to PostgreSQL

## What You Are Building

Day 1 read a file. A file sits still. You can download it, look at it, and download it again tomorrow and get the same thing.

Day 2 reads a live API instead, and everything that is harder about live sources shows up at once. The data keeps growing. It comes back a page at a time, so you have to keep asking for more. It arrives as XML rather than a neat table. One paper has many authors, which does not fit in a single row. And when you run the pipeline again tomorrow, you need it to pick up where it stopped rather than starting from the beginning.

You are pulling research papers from ArXiv across several AI categories, pulling them apart, and loading them into PostgreSQL in a way you can safely repeat.

## Words You Will Need

**API.** A way for one program to ask another program for data over the network. Instead of downloading a file, you send a request and get an answer back.

**Pagination.** The API will not hand you 10,000 papers at once. It gives you 100, and tells you there are more. Pagination is the loop that keeps asking for the next batch until you have what you need.

**Atom and XML.** ArXiv answers in XML, a text format that nests things inside other things, a bit like folders inside folders. Atom is a particular flavour of XML used for feeds. It is not a table, so you cannot hand it straight to pandas. You have to walk the structure and pull out the parts you want.

**Watermark.** A note the pipeline writes to itself saying "I got this far". Next run it reads that note and starts from there. Without it, every run would download everything again from the beginning.

**Child table.** A paper can have one author or forty. You cannot put forty authors in one column and still query them sensibly, so authors go in their own table with a column pointing back at the paper they belong to. That second table is the child table.

**Idempotent.** A run you can repeat without damage. Running it twice leaves the database in the same state as running it once. Day 1 got this from upserts, and Day 2 needs it even more, because pages overlap between runs on purpose.

## Before You Start

You need PostgreSQL running and your packages installed. If you have not set the machine up, do that in [STUDENT_ONBOARDING.md](../STUDENT_ONBOARDING.md) first.

Then:

```bash
docker compose up -d
```

## Run It

```bash
python day2/lesson.py
```

Be aware that a full run is slow on purpose. ArXiv is a free public service, and hammering it is rude, so the pipeline waits 5 seconds between pages. Fetching 10,000 papers means roughly 100 pages, so expect somewhere around 12 to 15 minutes. That waiting is not a bug and it is worth pointing at in class.

If you just want to see it work without the wait, set a smaller limit for one run:

```bash
ARXIV_MAX_PAPERS=300 python day2/lesson.py
```

The same pipeline also runs from here:

```bash
python day2/day2_arxiv_api_to_postgres.py
```

## What The Files Are

- `lesson.py`: the short version. Read this first.
- `pipeline/`: the work, split into config, extract, parse, transform, load, state, checks, and outputs.
- `day2_arxiv_api_to_postgres.py`: a second way to run the same pipeline.
- `DAY2_CODE_WALKTHROUGH.md`: the slower explanation.
- `output/`: rejected sample files.

## What Ends Up In The Database

Everything goes into the `training_data` schema, which Days 3, 4 and 5 all read from as well.

- `training_data.raw_arxiv_entries`: each entry as it arrived, before judgement.
- `training_data.clean_papers`: the papers that passed every check. This is the table the rest of the week is built on.
- `training_data.paper_authors`: one row per author per paper.
- `training_data.rejected_arxiv_entries`: entries that failed, with reasons.
- `training_data.pipeline_state`: the watermark, so the next run knows where to resume.

## The Checks The Pipeline Runs

An entry is rejected if it is missing any of: the paper id, the title, the summary, the published date, or the authors. A paper with no summary is useless to Day 5, so it is better to catch it here than to discover it three days later.

Two other things happen that are worth understanding:

The ArXiv id gets normalised. ArXiv hands out ids with version numbers on the end, like `2603.05743v2`. If you treated `v1` and `v2` as different papers you would load the same work twice. The pipeline strips the version so a paper keeps one identity across runs.

The original nested entry is kept in full. Same reason as Day 1. If the parser turns out to have a bug, you can fix it and re-parse without going back to the API.

## What To Look At Afterwards

- The JSON logs, especially the final counts of raw, clean, author and rejected rows.
- `day2/output/rejected_sample_<batch_id>.csv`.
- The row counts in all five tables.
- The watermark sitting in `training_data.pipeline_state`. Run the pipeline twice and watch it move.

## Settings For A Full Corpus

These are the values that build a corpus deep enough for Days 3 to 5. They go in your `.env`:

```env
ARXIV_CATEGORIES=cs.LG,cs.AI,cs.CV,cs.CL,stat.ML,cs.RO,cs.IR,cs.CR
ARXIV_SEARCH_QUERY=
ARXIV_SOURCE_NAME=arxiv_multi_domain
ARXIV_MAX_PAPERS=10000
ARXIV_PAGE_SIZE=100
ARXIV_SLEEP_SECONDS=5
ARXIV_DEFAULT_LOOKBACK_DAYS=180
ARXIV_OVERLAP_MINUTES=10
ARXIV_REQUEST_TIMEOUT_SECONDS=90
ARXIV_REQUEST_MAX_RETRIES=6
ARXIV_RETRY_BACKOFF_SECONDS=5
ARXIV_USER_AGENT=DataEngineeringForAI-Day2/1.0 (contact: instructor@example.com)
```

Leave `ARXIV_SEARCH_QUERY` blank and the pipeline builds the query for you from the category list.

`ARXIV_OVERLAP_MINUTES=10` is worth explaining. When the pipeline resumes, it deliberately goes back ten minutes before the watermark and re-reads that window. It sounds wasteful, but papers do not always appear in a tidy order, and a small overlap means you do not miss one that arrived late. The upserts make the repeated rows harmless.

## A Note For Teaching

Day 1 was a file. Day 2 is a feed, and it introduces the four problems that come with live sources: paging, parsing nested data, storing repeated values, and remembering where you stopped.

The watermark here is deliberately the simplest thing that works: the newest published timestamp, minus a small overlap. Real systems get more careful than this, but the idea is the same, and starting simple is what makes it explainable.
