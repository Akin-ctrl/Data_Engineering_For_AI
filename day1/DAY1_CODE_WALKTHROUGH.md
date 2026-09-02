# Day 1 Code Walkthrough

This is the slower version of the Day 1 README. It goes through the code one piece at a time.

Open `day1/lesson.py` first. Only open the files in `day1/pipeline/` when you want to see how one particular step works.

## The Shape Of The Day

Almost every data pipeline you will ever build has the same four moves:

```text
extract -> transform -> load -> check
```

Extract means get the data. Transform means fix it and decide what you trust. Load means put it somewhere it will stay. Check means prove the load actually worked instead of assuming it did.

For Day 1 those four look like this:

```text
CSV on the internet -> read with pandas -> split into clean and rejected -> PostgreSQL -> quality checks
```

The one idea to hold on to:

> Clean rows and rejected rows both get stored, on purpose.

Bad rows should not vanish silently, and they should not be allowed to sit in the clean table pretending to be fine. They go in a table of their own, with a note saying what was wrong with them.

## Two Words You Will See A Lot

**Upsert.** Short for "update or insert". You hand the database a row, and it either adds it or, if a row with that key is already there, updates the existing one. This is what lets you run the pipeline twice without ending up with two copies of everything. Running something twice and getting the same result is called being idempotent, and it is a property you want in almost every pipeline you build.

**Row hash.** A short fingerprint calculated from the contents of a row. Two rows with the same values produce the same fingerprint, and changing any value changes the fingerprint. It gives every row a stable identity even when the source file has no ID column, which is how we spot the same row arriving twice.

## The Entry Point

`day1/lesson.py` is the file to read in class. The main function is deliberately short, and it reads as a list of steps:

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

That is the whole day. Everything else is detail.

## Where Everything Lives

| File | What It Does |
|---|---|
| `day1/lesson.py` | The whole pipeline in readable order. |
| `day1/day1_hf_csv_to_postgres.py` | A second way to run the same pipeline. |
| `day1/pipeline/config.py` | Reads `.env` and checks the settings are there. |
| `day1/pipeline/constants.py` | The schema and table names, in one place. |
| `day1/pipeline/logging_utils.py` | Sets up the JSON log output. |
| `day1/pipeline/extract.py` | Works out the download URL, downloads the file, reads it. |
| `day1/pipeline/transform.py` | Tidies columns, hashes rows, decides what is rejected. |
| `day1/pipeline/load.py` | Creates the tables and upserts the rows. |
| `day1/pipeline/outputs.py` | Writes the rejected sample CSV. |
| `day1/pipeline/checks.py` | Runs the checks after loading. |

## Going Through It In Class

### 1. Start with `day1/lesson.py`

Read it top to bottom without opening anything else. The point is that a pipeline is a story with an order, and you should be able to follow that order before you look at any of the machinery.

### 2. Configuration

Open `day1/pipeline/config.py`.

Three things matter here:

- The settings come from the `.env` file, not from inside the code.
- If a required setting is missing, the pipeline stops immediately with a message saying which one.
- The database connection string is assembled from those settings.

That second point is worth pausing on. Failing at the start with "you forgot PGPASSWORD" is much kinder than failing forty seconds later with a connection error that does not say what is wrong.

> Settings do not belong inside pipeline logic.

### 3. Extract

Open `day1/pipeline/extract.py`.

- It works out the real download URL for the HuggingFace file.
- It downloads the CSV into `day1/output/`.
- It reads that file with pandas.

Notice what it does not do. It does not clean anything and it does not touch the database. Extract has one job.

> Extract gets data into memory. It does not clean it and it does not load it.

### 4. Transform

Open `day1/pipeline/transform.py`. This is the heart of Day 1.

Four functions to look at:

- `normalize_column_name` turns `Positive Feedback Count` into `positive_feedback_count`. Spaces and capital letters in column names cause trouble in SQL, so we get rid of them once, at the start.
- `stable_row_hash` builds the fingerprint described above.
- `prepare_raw_records_for_load` gets rows ready for the raw table, before any judgement is applied.
- `clean_dataset_defensively` is where every row is checked and sorted into clean or rejected.

Read `clean_dataset_defensively` slowly. Each rule is a few lines, and each failure appends a reason like `rating_out_of_range_1_to_5` to a list. A row can collect several reasons, and all of them get stored, because "this row is bad" is much less useful six months later than "this row is bad for these three reasons".

> Transformation is where you decide what is acceptable, what is not, and why.

### 5. Load

Open `day1/pipeline/load.py`.

Rows go to three places:

- the raw table, exactly as they arrived
- the clean table, if they passed everything
- the rejected table, with their reasons, if they did not

All three use upserts, so running the pipeline again updates rows instead of duplicating them.

Keeping the raw table costs disk space and buys you the ability to change your mind. If you decide next week that age 12 should be allowed after all, you can rebuild the clean table from raw without downloading anything.

> A good pipeline keeps the original rows, the accepted rows, and the rejected rows.

### 6. Check

Open `day1/pipeline/checks.py`.

After loading, it counts rows, looks for nulls where there should not be any, looks for duplicate keys, and prints how the ratings are distributed.

This step exists because writing to a database and writing the right thing to a database are different events. A pipeline that finishes without error can still have loaded nothing at all.

> A pipeline is not done when it writes data. It is done when it proves the write worked.

## Questions Students Ask

### Why keep the raw rows?

So you can answer questions later. Clean data on its own cannot tell you what you threw away or why. Raw rows let you audit, replay, and debug.

### Why keep the rejected rows?

Because they are evidence. A big pile of rejects means one of three things: the source data is broken, your rules are too strict, or your pipeline has a bug. You cannot tell which without looking at them.

### Why bother with row hashes?

They give each row a stable identity even when the file has no ID column. That is what makes deduplication and repeat runs possible.

### Why upserts instead of plain inserts?

So you can run the script twice without doubling your data. That sounds minor until the first time a run half fails and you need to run it again.

### Why split this into so many small files?

Because each file now has one job: config, extract, transform, load, output, check. When something breaks, the file name tells you where to look.

## The Short Version

> Day 1 takes a public CSV and treats it like production data. We do not trust it. We download it, read it carefully, keep the original, check every row, send the good rows to a clean table and the bad rows to a rejected table with their reasons, and then run checks to prove the load actually worked.
