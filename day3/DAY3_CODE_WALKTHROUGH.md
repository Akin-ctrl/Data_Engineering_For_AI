# Day 3 Code Walkthrough

This walkthrough explains how the Day 3 pipeline turns PostgreSQL data into CSV and Parquet, then measures the real difference in read performance and file size.

## 0. Imports and Libraries

Every import in the script has a specific purpose:

- `gc`: forces a garbage-collection pass before each benchmark run to reduce memory noise.
- `json`: writes the benchmark report to disk.
- `logging`: records structured runtime logs.
- `os`: reads environment variables.
- `re`: validates SQL identifier-like settings.
- `time`: measures elapsed time with nanosecond precision.
- `dataclasses.dataclass`: defines the typed config and result containers.
- `datetime`, `timezone`: timestamps the report and logs.
- `pathlib.Path`: manages output paths.
- `statistics.mean` and `statistics.median`: compute benchmark summary values.
- `typing.Any` and `typing.Callable`: keep the code explicit and typed.
- `pandas as pd`: loads SQL results, writes CSV, and prints the summary table.
- `pyarrow as pa`: converts the pandas frame into an Arrow table for Parquet writing.
- `pyarrow.csv as pacsv`: benchmarks direct CSV reads with PyArrow.
- `pyarrow.parquet as pq`: writes Parquet and benchmarks direct Parquet reads.
- `dotenv.load_dotenv`: loads local environment variables.
- `sqlalchemy.create_engine` and `sqlalchemy.text`: connect to PostgreSQL and run the source query.
- `sqlalchemy.engine.Engine`: documents the engine type.

The structure is intentionally straightforward: pandas handles the dataframe workflow, PyArrow handles the columnar format, and SQLAlchemy handles the database connection.

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

The `Day3Config` class packages these values into one typed object so the rest of the script stays readable.

## 5. Logging

The script uses JSON logs, just like the earlier labs. That keeps progress messages structured and easy to inspect.

The `JsonFormatter` class creates the JSON log record, and `configure_logger()` installs it on the `day3_benchmark` logger.

## 6. The Source Query

The source query reads from `training_data.clean_papers` and orders rows by `paper_key`.

It also casts JSONB columns to text so CSV and Parquet export the same logical content.

The query itself is built by `build_source_sql()`, and the data is loaded by `load_source_frame()` using `pd.read_sql_query()`.

## 7. Why We Keep the Export Deterministic

Every run overwrites the same output files.

That makes the lab easy to explain because students can rerun it and compare the same filenames.

## 8. Writing CSV

The script uses pandas to write the export frame to UTF-8 CSV.

CSV is the most familiar interchange format, but it is not the best choice for analytical workloads.

That work is done inside `write_exports()`, which first writes the CSV path from `Day3Config.csv_path`.

## 9. Writing Parquet With PyArrow

The Parquet file is written using PyArrow.

That matters because the lab is specifically showing a modern columnar export path, not just a generic dataframe save.

The same `write_exports()` function converts the pandas dataframe to an Arrow table with `pa.Table.from_pandas()` and then writes Parquet using `pq.write_table()`.

## 10. Why Snappy Compression

Snappy is a good teaching default.

It gives a meaningful size reduction without making the example harder to understand or slower to write.

## 11. Benchmark Warmup

Before the timed runs begin, the script performs a warmup read for each reader.

That reduces first-run noise and keeps the benchmark focused on file format differences instead of import overhead.

The warmup pass happens inside `run_benchmarks()`, before the timed runs start.

## 12. Benchmark Readers

The script measures four reads:

- `pandas.read_csv`
- `pandas.read_parquet` with the `pyarrow` engine
- `pyarrow.csv.read_csv`
- `pyarrow.parquet.read_table`

This gives learners both a pandas view and a direct PyArrow view of the same data.

The four readers are defined in `run_benchmarks()` and timed by `measure_reader()`.

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

The report is assembled by `build_report()` and written by `write_report()`.

## 16. What Students Should Notice

If everything is working correctly, Parquet should be smaller than CSV.

For read speed, pandas usually shows the clearest Parquet win on this dataset, while the PyArrow comparison reminds students that the reader library also affects timing.

The exact numbers depend on the machine and cache state, which is why the lab prints the real measured results instead of making a guess.

## 17. Teaching Point

The most important lesson is that storage format is part of pipeline design.

When a dataset is read repeatedly, a better file format can save both time and space with very little extra complexity.

## 18. Function Map

This is the full function map so learners can trace the file from top to bottom:

- `configure_logger()`: creates the JSON logger.
- `validate_identifier()`: ensures SQL-safe identifier formatting.
- `parse_positive_int()`: validates integer settings.
- `resolve_relative_path()`: maps relative output folders to the repository root.
- `load_config()`: loads and validates environment settings.
- `Day3BenchmarkPipeline.load_source_frame()`: reads the PostgreSQL source table.
- `Day3BenchmarkPipeline.build_source_sql()`: builds the source query.
- `Day3BenchmarkPipeline.prepare_export_frame()`: copies the dataframe into a stable export frame.
- `Day3BenchmarkPipeline.write_exports()`: writes CSV and Parquet files.
- `Day3BenchmarkPipeline.run_benchmarks()`: runs the warmup and timed read tests.
- `Day3BenchmarkPipeline.measure_reader()`: times one reader/file-format pair.
- `Day3BenchmarkPipeline.build_report()`: assembles the JSON benchmark report.
- `Day3BenchmarkPipeline.build_summary_rows()`: prepares the classroom comparison rows.
- `Day3BenchmarkPipeline.write_report()`: writes the JSON report.
- `Day3BenchmarkPipeline.print_summary_table()`: prints the console table.
- `main()`: runs the pipeline from the command line.

## 19. Class Map

The Day 3 file uses three dataclasses and one pipeline class:

- `Day3Config`: holds all configuration, paths, and connection settings.
- `ReadBenchmarkResult`: stores the timing metrics for one reader and one file format.
- `FileArtifact`: stores the file path and size metadata for an export.
- `Day3BenchmarkPipeline`: owns the full export and benchmark workflow.
