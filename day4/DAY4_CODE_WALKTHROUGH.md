# Day 4 Code Walkthrough

This walkthrough explains how the Day 4 workflow wraps the Day 2 and Day 3 labs into one orchestrated sequence with retry handling.

## 0. Imports and Libraries

The Day 4 orchestrator uses these imports:

- `json`: writes the orchestration report.
- `logging`: records structured runtime logs.
- `os`: reads environment variables.
- `subprocess`: runs the Day 2 and Day 3 scripts as child commands.
- `sys`: passes the current Python interpreter into child commands.
- `time`: sleeps between outer workflow retry attempts.
- `dataclasses.dataclass` and `dataclasses.asdict`: define structured report objects and convert them to JSON-ready dictionaries.
- `datetime`, `timezone`: timestamp log messages and reports.
- `pathlib.Path`: resolves file and directory paths.
- `typing.Any`: keeps the snapshot/report typing explicit.
- `dotenv.load_dotenv`: loads `.env` settings.
- `prefect.flow`, `prefect.task`, `prefect.get_run_logger`: provide the orchestration framework and retryable tasks.
- `sqlalchemy.create_engine` and `sqlalchemy.text`: connect to PostgreSQL and gather final counts for the report.
- `sqlalchemy.engine.Engine`: documents database engine types in helper functions.

Prefect is the orchestrator here. It gives the lab task retries, flow structure, and readable run logs without requiring a large amount of infrastructure.

If you start the local Prefect server with `prefect server start`, the UI becomes the visual companion for this lab.

## 1. Why Day 4 Exists

Day 4 turns the earlier labs into a single pipeline. The point is to show sequencing, orchestration, and recovery instead of manually running each step yourself.

## 2. The Orchestration Choice

The lab uses Prefect because it is lightweight and readable.

That keeps the code close to the teaching goal:

- run one step after another
- retry failed tasks automatically
- retry the whole workflow if a larger failure happens

In this implementation, Prefect is used through the `@task` and `@flow` decorators, plus `get_run_logger()` for task-scoped log messages.

The UI is where students can see those decorators pay off: the flow appears as a run, each task appears as a node, and failed attempts are visible before the retry succeeds.

Day 4 now also supports Prefect-native scheduling. When schedule mode is enabled in `.env`, the script serves a recurring Prefect deployment instead of running once.

## 3. What Gets Reused

The workflow does not rewrite Day 2 or Day 3 logic.

Instead, it calls the existing scripts:

- `day2/day2_arxiv_api_to_postgres.py`
- `day3/day3_agent_query_views.sql`
- `day3/day3_postgres_to_csv_parquet_benchmark.py`

That keeps the earlier labs intact and makes Day 4 easy to understand.

The workflow reuses the existing scripts instead of importing their internals, which keeps the orchestration layer simple and avoids duplicating Day 2 and Day 3 logic.

## 4. Execution Order

The workflow runs in this order:

1. Day 2 API ingestion.
2. Day 3 SQL view provisioning.
3. Day 3 export and benchmark.
4. Day 4 orchestration report.

The Parquet file is created during the last step in that sequence.

The actual commands are run through `subprocess.run()` inside helper functions, so Day 4 acts as a wrapper around the already working scripts.

## 5. Task-Level Retries

The Day 2, view-provisioning, and Day 3 export steps are each wrapped as Prefect tasks.

Each task has retry settings, so a temporary error can be retried without restarting the entire pipeline.

Those retries are configured at the task decorator level with `retries=TASK_RETRIES` and `retry_delay_seconds=TASK_RETRY_DELAY_SECONDS`.

## 6. Workflow-Level Retry

The full flow is also wrapped in a small outer retry loop.

That matters for failures that happen after the tasks complete, such as a final validation failure or an injected sabotage checkpoint.

The outer retry loop lives in `run_with_workflow_retries()`, which wraps the full Prefect flow and sleeps between attempts.

## 7. Sabotage Hook

The lab includes a deliberate failure hook controlled by environment variables.

The hook can fail once and leave a marker file behind, which makes the next retry succeed.

That is useful for teaching because students can see the retry behavior instead of just reading about it.

The sabotage behavior is controlled by `DAY4_SABOTAGE_TARGET` and `DAY4_SABOTAGE_MODE`, and the failure is injected by `maybe_trigger_sabotage()`.

In the Prefect UI, that sabotage shows up as a failed task or flow attempt first, followed by a retried run when the marker file allows the next attempt to continue.

## 8. Why the Marker File Helps

The marker file makes the failure deterministic.

Without it, retries can be hard to explain because the failure may or may not happen again in the same way.

With it, the first attempt fails on purpose and the retry succeeds.

## 9. Day 4 Report

The workflow writes a report to `day4/output/` so you can review the run later.

The report summarizes:

- which steps ran
- how many attempts were needed
- where the Day 2 and Day 3 artifacts were written
- whether sabotage was enabled

The report is written by `write_day4_report()` and includes step results, database counts, and a summary of the Day 3 benchmark report.

That report gives the classroom a second place to inspect the run after the UI, especially if the UI is not open anymore.

## 10. Teaching Value

This lab gives students the main orchestration ideas without overwhelming them:

- step ordering
- retries
- failure boundaries
- idempotent downstream work

That is enough to make the next conversation about production orchestration much easier.

## 11. Function Map

Here is the full function map for the Day 4 file:

- `configure_logger()`: creates the JSON logger.
- `parse_positive_int()`: validates integer configuration values.
- `resolve_relative_path()`: resolves relative output paths.
- `validate_choice()`: checks the sabotage settings against allowed values.
- `parse_bool()`: parses boolean schedule flags from environment values.
- `load_config()`: loads and validates Day 4 environment settings.
- `resolve_schedule_seconds()`: converts the configured day/hour schedule into seconds.
- `ensure_paths_exist()`: checks that the Day 2 and Day 3 files exist before the workflow starts.
- `run_subprocess()`: runs a child script or SQL command.
- `maybe_trigger_sabotage()`: injects a controlled failure for retry demos.
- `run_day2_pipeline()`: Prefect task that runs the Day 2 ingestion script.
- `provision_day3_views()`: Prefect task that creates the Day 3 views and materialized views.
- `run_day3_export()`: Prefect task that runs the Day 3 export and benchmark script.
- `day4_flow()`: the Prefect flow that runs the three steps in order.
- `collect_database_snapshot()`: gathers final row counts for the report.
- `load_day3_report_summary()`: loads the Day 3 benchmark summary if it exists.
- `write_day4_report()`: writes the final orchestration report.
- `run_with_workflow_retries()`: wraps the flow in outer retries.
- `day4_scheduled_flow()`: scheduled wrapper flow used by Prefect native schedule mode.
- `serve_prefect_schedule()`: serves the scheduled deployment with a configurable interval.
- `main()`: command-line entry point.

## 12. Class Map

The Day 4 file uses three dataclasses:

- `Day4Config`: stores configuration, paths, retry settings, and sabotage settings.
- `WorkflowStepResult`: stores the outcome of one orchestration step.
- `WorkflowReport`: stores the final JSON summary written at the end of the run.
