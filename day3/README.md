# Day 3 Lab: PostgreSQL to CSV and Parquet

## What You Are Building

The papers are in the database. Now you take them out, write them to disk in two different file formats, and measure which one is better and in what way.

This sounds like a small day, and the code is small. The point is the result, which is more interesting than most people expect and does not say what everyone assumes it will say.

## Words You Will Need

**ETL and ELT.** Two orders for the same three jobs. ETL is Extract, Transform, Load: pull the data out, fix it up in your own code, then put it somewhere. ELT swaps the last two: pull it out, load it, and let the database do the transforming. Day 1 and Day 2 were closer to ETL, because pandas and Python did the cleaning. Day 3 is ELT, because the transforming happens in SQL inside PostgreSQL.

**View.** A saved query. It looks like a table when you select from it, but it holds no data of its own, it just runs its query when you ask. It is a way to give a complicated query a name.

**Materialized view.** A view that does keep its results, like a snapshot. Reading from it is fast because the work is already done, but the snapshot goes stale until something refreshes it. You use one when the query is expensive and the data does not change every minute.

**CSV.** A text file where each line is a row and commas separate the values. Every program on earth can read it. It has no types, no compression, and no structure beyond lines and commas.

**Parquet.** A file format built for analytics. Instead of storing row by row, it stores column by column, compresses each column, and keeps an index at the end saying where each column starts.

**Column projection.** Asking for only some of the columns instead of all of them. Nearly every real query does this, and it turns out to be the whole story of Day 3.

## Before You Start

You need Day 2 to have run, because Day 3 reads the table Day 2 filled. If `training_data.clean_papers` is empty, go back and run Day 2 first.

```bash
docker compose up -d
```

## Run It

```bash
python day3/lesson.py
```

The same pipeline also runs from here:

```bash
python day3/day3_postgres_to_csv_parquet_benchmark.py
```

## What Happens, In Order

1. Read the clean papers out of PostgreSQL.
2. Create the SQL views inside the database. This is the Transform.
3. Write the same rows to a CSV file and a Parquet file.
4. Read both files back, over and over, with a stopwatch running.
5. Print a table and save a JSON report.

Step 2 finishes before step 3 starts. The Transform genuinely happens before the Load, which is the order the ELT name describes.

## The Result, And Why It Surprises People

Here is the shape of what you get:

```text
read_mode  reader  columns  csv_avg_ms  parquet_avg_ms  speedup_factor
     full  pandas       19     748.363         144.845           5.167
     full pyarrow       19      51.671         106.811           0.484
projected  pandas        3     379.469           6.706          56.586
projected pyarrow        3      32.951           3.622           9.097
```

Your exact numbers will differ, since they depend on your machine and how many
papers you loaded. The shape of the result is what matters, and that stays the
same.

`speedup_factor` above 1 means Parquet won. Below 1 means CSV won.

Look at the second row. Reading every column with PyArrow, the CSV is **faster** than the Parquet file. That is not a mistake in the benchmark, and you should not skip past it.

The reason is that PyArrow has a very good CSV reader that uses several CPU cores at once. When you ask for the whole file, that reader is hard to beat, and Parquet has to spend time undoing its own compression. Being smaller on disk does not automatically make a file faster to read.

Now look at the bottom two rows, where we ask for three columns out of nineteen. Parquet wins for both readers, and it wins enormously. Here is why:

- A CSV is one long piece of text with no map. To find the third column of every row, the reader still has to walk through and parse every single character of the file, including the huge summary and payload columns you did not ask for.
- Parquet keeps each column in its own block, and keeps a small index at the end saying where each block starts. Ask for three columns and it reads three blocks. The rest of the file is never touched.

So the honest lesson is not "Parquet is faster". It is:

> Parquet is always smaller, and it is dramatically faster when you read part of your data. Real analytics queries almost always read part of the data, which is why Parquet wins in practice.

That is a more useful thing to know than the version where the numbers all point the same way.

## Where The Files Go

Reading from `training_data.clean_papers` by default and writing:

- `day3/output/day3_clean_papers_benchmark.csv`
- `day3/output/day3_clean_papers_benchmark.parquet`
- `day3/output/day3_clean_papers_benchmark.json`

Each run overwrites them, and the same input always produces the same output.

## What The Files Are

- `lesson.py`: the short version. Read this first.
- `pipeline/`: config, source loading, exports, SQL provisioning, benchmarks, reports, logging.
- `day3_agent_query_views.sql`: the SQL views, which are the Transform step.
- `day3_postgres_to_csv_parquet_benchmark.py`: a second way to run the same pipeline.
- `DAY3_CODE_WALKTHROUGH.md`: the slower explanation.
- `output/`: the CSV, Parquet and JSON that come out.

## Settings

```env
DAY3_SOURCE_TABLE=training_data.clean_papers
DAY3_ORDER_BY=paper_key
DAY3_OUTPUT_DIR=day3/output
DAY3_EXPORT_BASENAME=day3_clean_papers_benchmark
DAY3_BENCHMARK_RUNS=10
```

`DAY3_BENCHMARK_RUNS` is how many times each read is repeated. Timing something once tells you very little, because your machine might have been busy for that one moment. Ten runs and taking the average is the cheapest way to get a number you can trust.

## What To Look At Afterwards

- The summary table, especially the gap between the `full` and `projected` rows.
- The JSON report in `day3/output/`, which has the full timings.
- The two files on disk. Parquet is a little under half the size.

## About The SQL Views

The views are created by this lab as its Transform step, and Day 4 creates them again when it runs its full workflow. Running either is enough. Creating them twice does no harm, because the SQL drops and recreates them each time.
