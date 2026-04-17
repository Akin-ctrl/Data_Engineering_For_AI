# Day 4 Lab: Orchestrated Workflow With Retries

## Goal

Wrap the Day 2 API ingestion and the Day 3 export/benchmark work into one orchestrated workflow.

This lab is about sequencing and recovery:

- API extract happens first.
- PostgreSQL loading happens second.
- Day 3 analytical views and benchmark outputs happen after the data is in place.
- The Parquet file is generated last inside the Day 3 export step.
- Retries handle both step failures and a full workflow failure.

## What This Day Covers

- Running Day 2 and Day 3 as one pipeline.
- Using Prefect for beginner-friendly orchestration.
- Adding task-level retries.
- Adding an outer workflow retry loop.
- Simulating a failure on purpose and watching the retry recover.
- Preserving the Day 2 and Day 3 artifacts while adding a Day 4 report.

## Files

- `day4_orchestrated_workflow.py`: the orchestrated workflow entry point.
- `README.md`: quickstart for the lab.
- `DAY4_CODE_WALKTHROUGH.md`: step-by-step explanation of the orchestration logic.

## Install Dependencies

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Run the Day 4 Workflow

```bash
python day4/day4_orchestrated_workflow.py
```

This command runs the workflow once by default.

## Prefect Native Schedule (Every 24 Hours by Default)

Day 4 can run in Prefect-native scheduled mode.

Set these values in `.env`:

```env
DAY4_SCHEDULE_ENABLED=true
DAY4_SCHEDULE_EVERY_HOURS=24
DAY4_SCHEDULE_EVERY_DAYS=0
DAY4_SCHEDULE_NAME=day4-orchestrated-every-24h
```

Then start the scheduled worker process:

```bash
python day4/day4_orchestrated_workflow.py
```

When schedule mode is enabled, this process serves a Prefect deployment with a repeating interval.

Interval rules:

- If `DAY4_SCHEDULE_EVERY_DAYS` is greater than 0, it is used.
- Otherwise, `DAY4_SCHEDULE_EVERY_HOURS` is used.
- This makes it easy for students to tweak cadence in hours or whole days.

## Prefect UI

Prefect includes a local UI, which is useful for student immersion because it lets you watch task states, retries, logs, and failures in one place.

Start the UI in a separate terminal:

```bash
prefect server start
```

Then open the URL Prefect prints, usually `http://127.0.0.1:4200`.

What students should look at:

- The flow run page for the Day 4 orchestration.
- The task cards for Day 2, Day 3 views, and Day 3 export.
- Retry attempts when the sabotage hook is enabled.
- The logs panel for step-by-step status messages.

If Prefect is not already connected to the local server, run the Day 4 script from the same shell session after starting the server.

## Default Behavior

By default, the workflow:

1. Runs the Day 2 ingestion pipeline.
2. Creates the Day 3 query views and materialized views.
3. Runs the Day 3 CSV/Parquet benchmark.
4. Writes a Day 4 orchestration report to `day4/output/`.

## Retry Demo

The sabotage hook is controlled through environment variables.

Recommended settings:

```env
DAY4_OUTPUT_DIR=day4/output
DAY4_WORKFLOW_RETRIES=2
DAY4_WORKFLOW_RETRY_DELAY_SECONDS=10
DAY4_TASK_RETRIES=2
DAY4_TASK_RETRY_DELAY_SECONDS=5
DAY4_SABOTAGE_TARGET=off
DAY4_SABOTAGE_MODE=once
DAY4_SCHEDULE_ENABLED=false
DAY4_SCHEDULE_EVERY_HOURS=24
DAY4_SCHEDULE_EVERY_DAYS=0
DAY4_SCHEDULE_NAME=day4-orchestrated-every-24h
```

To simulate a task retry, set:

```env
DAY4_SABOTAGE_TARGET=day3_export
```

To simulate a full workflow retry, set:

```env
DAY4_SABOTAGE_TARGET=workflow_checkpoint
```

The workflow UI will show the failure on the first attempt and the successful retry on the next attempt when `DAY4_SABOTAGE_MODE=once`.

## What To Watch

- Prefect logs for task attempts.
- The Day 4 report in `day4/output/`.
- The existing Day 2 and Day 3 outputs.
- The retry behavior when the sabotage hook is enabled.

## Teaching Note

This lab is useful because it shows three production ideas at once:

- Pipeline steps should be ordered deliberately.
- Retries should be local when possible.
- A workflow should still be able to restart from a bigger failure boundary.
