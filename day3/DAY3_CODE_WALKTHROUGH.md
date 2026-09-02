# Day 3 Code Walkthrough

The slower version of the Day 3 README. Open `day3/lesson.py` first, and go into `day3/pipeline/` only when you want to see how one step works.

If you have not read the "Words You Will Need" section in [README.md](README.md), read that first. This file assumes you know what a view is and what column projection means.

## The Shape Of The Day

```text
PostgreSQL clean table -> create SQL views -> write CSV and Parquet -> time the reads -> report
```

Days 1 and 2 were mostly ETL: get the data, fix it in Python, put it in the database. Day 3 is ELT, and the difference is where the Transform happens.

> In ELT the Transform happens in the database, after the data is already in there, not in Python before it goes in.

That is not a style preference. Day 2 already left clean papers in PostgreSQL. Pulling them into Python just to reshape them and push them back would be pointless work. The database is right there and it is good at this, so we let it do the job.

## Where Everything Lives

| File | What It Does |
|---|---|
| `day3/lesson.py` | The whole pipeline in readable order. |
| `day3/day3_postgres_to_csv_parquet_benchmark.py` | A second way to run the same pipeline. |
| `day3/day3_agent_query_views.sql` | The SQL views. This is the Transform step. |
| `day3/pipeline/config.py` | Reads `.env` and checks the settings. |
| `day3/pipeline/constants.py` | Defaults, and the list of columns used for the projected read. |
| `day3/pipeline/logging_utils.py` | Sets up the JSON log output. |
| `day3/pipeline/source.py` | The SELECT that pulls the clean papers out. |
| `day3/pipeline/provision.py` | Runs the SQL views file against the database. |
| `day3/pipeline/exports.py` | Writes the CSV and the Parquet file. |
| `day3/pipeline/benchmark.py` | Times the reads, over and over. |
| `day3/pipeline/report.py` | Builds the JSON report and prints the summary table. |

## Going Through It In Class

### 1. Start with `day3/lesson.py`

```text
load config
create the database connection
load the clean source data
prepare the export frame
provision the SQL views       (Transform, in the database)
write CSV and Parquet         (Load, to disk)
time the reads
build the report
print the summary
```

The order matters and is worth a sentence. Transform runs before Load, which is the sequence the ELT name actually describes. The two steps are independent enough that a thread pool could run them together, and it would save a fraction of a second, but then the diagram on the board would say one thing and the code would say another. Reading clearly is worth more than the fraction of a second.

### 2. The Source

Open `day3/pipeline/source.py`.

- It reads `training_data.clean_papers`, the table Day 2 filled.
- It orders the rows, so two runs produce identical files.
- It turns the JSONB columns into text, because CSV has no idea what JSON is.
- The result comes back as a pandas DataFrame.

The ordering is worth calling out. If the rows came back in a different order each run, the two files would differ every time and you could not compare anything. Any benchmark needs its input to be identical run to run.

> Day 3 trusts Day 2's clean table. The Extract step is a single SELECT.

### 3. The SQL Transform

Open `day3/day3_agent_query_views.sql`.

These views are saved queries that make the paper data easy to ask questions of:

- `v_agent_papers`: the main paper surface for retrieval
- `v_agent_category_counts`: how many papers per category
- `v_agent_author_frequency`: which authors appear most
- `mv_agent_keyword_frequency`: a materialized keyword index, so the expensive counting happens once
- `v_agent_pipeline_health`: a single row summarising whether the pipeline looks healthy

Then open `day3/pipeline/provision.py` to see how that file gets run. It reads the SQL and sends it to PostgreSQL through SQLAlchemy.

It is worth saying why it works that way. The obvious approach is to shell out to the `psql` command line tool, and plenty of projects do. The problem here is that PostgreSQL lives inside Docker, so a student has no reason to have `psql` installed on their own machine, and the failure when they do not have it is an unhelpful "file not found" buried under a retry. Running the SQL through the database connection we already have removes that whole class of problem.

There is a bonus. The SQL file wraps itself in `BEGIN` and `COMMIT`, which means if any statement in the middle fails, PostgreSQL undoes all of it. You never end up with half the views created.

> The Transform step in ELT is SQL running inside the database, after the data is already there.

### 4. The Exports

Open `day3/pipeline/exports.py`.

- `frame.to_csv(...)` writes the text file.
- `pa.Table.from_pandas(...)` converts the DataFrame into an Arrow table.
- `pq.write_table(..., compression="snappy")` writes the Parquet file.
- Then it records how big each file turned out.

Snappy is a compression setting. It does not squeeze as hard as some alternatives, but it compresses and decompresses very fast, which is usually the right trade for analytics files that get read a lot.

### 5. The Benchmark

Open `day3/pipeline/benchmark.py`.

Two readers, pandas and PyArrow. Two formats, CSV and Parquet. That is four combinations, and each one is run twice: once reading all nineteen columns, once reading only three. Eight cases in total, each repeated ten times.

Two details in the code worth pointing at:

- Everything is read once before any timing starts. The first read of a file is slower because the operating system has not cached it yet, and timing that would make whichever case ran first look unfairly bad.
- `gc.collect()` runs before each timed read, so Python's garbage collector does not fire in the middle of one measurement and not another.

Small things, but a benchmark that does not control for them is measuring noise.

### 6. The Part Students Ask About

When you read every column the result is not what people expect, and you should not skip past it.

pandas reads Parquet several times faster than CSV, which is the answer everyone predicts. PyArrow does the opposite: its CSV read beats its Parquet read.

The reason is that PyArrow's CSV reader is genuinely excellent and uses several CPU cores at once. When you ask for the entire file it is hard to beat, and Parquet has to spend time undoing its compression. Being smaller on disk does not automatically make a file faster to read.

Now look at the projected rows, where only three columns out of nineteen are requested. Parquet wins for both readers, and it wins hugely.

- A CSV is one long piece of text with no map of where anything is. To get the third column of every row, the reader still has to walk and parse every character in the file, including the enormous summary and payload columns nobody asked for.
- Parquet keeps each column in its own block and stores a small index at the end saying where each block begins. Ask for three columns and it reads three blocks. The rest is never touched.

> Parquet is always smaller, and it is much faster when you read part of your data. Real analytics queries almost always read part of the data, which is why Parquet wins in practice.

That is a better lesson than the tidy version where every number points the same way, because it is the one that is true.

### 7. The Report

Open `day3/pipeline/report.py`.

It records the source row count, the file sizes, every timing, and a summary row per reader for each of the two read modes. All of it goes to JSON as well as the printed table.

> A benchmark nobody wrote down is a benchmark nobody can check.

## What To Skip The First Time

Leave these until someone asks:

- every column in the SELECT
- the timing helper internals
- why garbage collection is forced before each read
- PyArrow internals
- every key in the JSON report
- every line of the SQL views

## Questions Students Ask

### Why is Day 3 ELT and not ETL?

Because Day 2 already left clean data in PostgreSQL. There is nothing to fix before loading. The Transform happens in the database, on data that is already sitting there.

### Why not use `psql` to run the SQL file?

Because PostgreSQL is in Docker, so students have no reason to have the `psql` program installed. Running the SQL through the connection we already have means one less thing to install and one less confusing error.

### Why compare CSV against Parquet at all?

Because CSV is what you get handed, and Parquet is what you should usually store. Knowing exactly what you gain, and where you do not gain anything, is the difference between following advice and understanding it.

### Why use two different readers?

To show that speed depends on the library as well as the file format. If we only measured pandas, we would have drawn a much simpler and slightly wrong conclusion.

### Why repeat every read ten times?

One measurement can be thrown off by anything else your machine was doing at that moment. Repeating and averaging is the cheapest way to get a number worth trusting.

### Why do the output files always have the same names?

Because this lab is about comparison. Same names mean you always know which files to look at, and each run replaces the last.

## The Short Version

> Day 3 follows the ELT pattern. We Extract the clean papers from PostgreSQL, Transform the database by creating SQL views inside it, then Load the same rows out to CSV and Parquet. Then we time how long each format takes to read back, both when we want every column and when we want only a few, and we save the numbers. The surprise is that Parquet does not always win on speed. It wins on size always, and on speed when you read part of your data, which is what real queries do.
