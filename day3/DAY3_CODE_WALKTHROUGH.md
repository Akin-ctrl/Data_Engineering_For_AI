# Day 3 Code Walkthrough

This walkthrough explains how the Day 3 pipeline turns PostgreSQL data into CSV and Parquet, then measures the real difference in read performance and file size.

## 1. Why Day 3 Exists

Day 3 is the "prove the math" lab. The goal is not only to export data, but also to show why file format choice matters for analytics.

## 2. Why We Start From PostgreSQL

The Day 2 lab already cleaned and normalized the data. That means Day 3 can focus on the storage layer instead of cleaning logic.

## 3. The Core Idea

The same data is written to two formats:

- CSV for simplicity and portability
- Parquet for analytical performance

Then the script reads both formats several times and compares the results.

## 4. Configuration

The script reads its settings from environment variables so students can adjust the lab without editing code.

Important values:

- `DAY3_SOURCE_TABLE`: the PostgreSQL table to query
- `DAY3_ORDER_BY`: the stable sort column for deterministic exports
- `DAY3_OUTPUT_DIR`: where the benchmark files are written
- `DAY3_EXPORT_BASENAME`: the shared file name prefix
- `DAY3_BENCHMARK_RUNS`: how many times each reader is timed

## 5. Logging

The script uses JSON logs, just like the earlier labs. That keeps progress messages structured and easy to inspect.

## 6. The Source Query

The source query reads from `training_data.clean_papers` and orders rows by `paper_key`.

It also casts JSONB columns to text so CSV and Parquet export the same logical content.

## 7. Why We Keep the Export Deterministic

Every run overwrites the same output files.

That makes the lab easy to explain because students can rerun it and compare the same filenames.

## 8. Writing CSV

The script uses pandas to write the export frame to UTF-8 CSV.

CSV is the most familiar interchange format, but it is not the best choice for analytical workloads.

## 9. Writing Parquet With PyArrow

The Parquet file is written using PyArrow.

That matters because the lab is specifically showing a modern columnar export path, not just a generic dataframe save.

## 10. Why Snappy Compression

Snappy is a good teaching default.

It gives a meaningful size reduction without making the example harder to understand or slower to write.

## 11. Benchmark Warmup

Before the timed runs begin, the script performs a warmup read for each reader.

That reduces first-run noise and keeps the benchmark focused on file format differences instead of import overhead.

## 12. Benchmark Readers

The script measures four reads:

- `pandas.read_csv`
- `pandas.read_parquet` with the `pyarrow` engine
- `pyarrow.csv.read_csv`
- `pyarrow.parquet.read_table`

This gives learners both a pandas view and a direct PyArrow view of the same data.

## 13. Timing Method

Each reader is timed across `DAY3_BENCHMARK_RUNS` repetitions.

For each reader, the script records:

- average read time in milliseconds
- median read time in milliseconds
- minimum and maximum read time
- row count returned by the reader

## 14. File Size Math

The script compares file sizes in megabytes.

It also calculates the size reduction percentage from CSV to Parquet so the storage difference is easy to explain.

## 15. What the JSON Report Stores

The JSON benchmark report includes:

- source table metadata
- export file paths and sizes
- detailed timing results
- the summary comparison rows used in the console

## 16. What Students Should Notice

If everything is working correctly, Parquet should be smaller than CSV.

For read speed, pandas usually shows the clearest Parquet win on this dataset, while the PyArrow comparison reminds students that the reader library also affects timing.

The exact numbers depend on the machine and cache state, which is why the lab prints the real measured results instead of making a guess.

## 17. Teaching Point

The most important lesson is that storage format is part of pipeline design.

When a dataset is read repeatedly, a better file format can save both time and space with very little extra complexity.
