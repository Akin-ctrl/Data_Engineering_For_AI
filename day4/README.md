# Day 4 Lab: Orchestrated Workflow With Retries

## What You Are Building

You have three things that work: Day 2 pulls papers, the SQL views get created, Day 3 exports and benchmarks. So far you have been running them by hand, in the right order, and hoping nothing fails.

Day 4 replaces you with a workflow. It runs the three steps in order, and when one of them fails it tries again instead of giving up. Then you break one on purpose and watch it recover.

## Words You Will Need

**Orchestration.** Running several jobs in the right order, handling what happens when one fails. If you have ever run three scripts one after another and had to remember which order they go in, you have done orchestration by hand. Day 4 makes the computer do it.

**Prefect.** The Python library doing the orchestration here. You describe your steps, and it runs them, tracks whether each one worked, retries the ones that did not, and gives you a web page to watch it all happen.

**Task.** One step in the workflow. Day 4 has three: run Day 2, create the views, run the Day 3 export.

**Flow.** The whole workflow, made of tasks. Prefect uses this word for the thing that ties the steps together.

**Retry.** Trying a failed step again. Networks glitch, databases are briefly busy, and a lot of failures fix themselves if you wait five seconds and try once more. A retry is that wait-and-try-again, done automatically.

**Sabotage hook.** A switch built into this lab that makes a step fail on purpose, so you can watch the retry work. It is not something you would ship, it is a teaching device.

## Why Two Levels Of Retry

This confuses people, so it is worth being clear up front.

A **task retry** is local. The Day 3 export failed, so try just the Day 3 export again. Nothing else re-runs.

A **workflow retry** is the bigger hammer. Something went wrong that retrying one step will not fix, so start the whole thing over from the beginning.

You want both. Most failures are small and local, and re-running everything for a hiccup in one step is wasteful. But some failures leave things in a state where only a clean start helps. Task retries handle the common case cheaply, and the workflow retry catches what they cannot.

## Before You Start

Day 4 runs Day 2, so everything Day 2 needs has to be working. The database must be up.

```bash
docker compose up -d
```

Be aware Day 4 takes as long as Day 2 does, because it runs it. Expect 12 to 15 minutes for a full run. To see the orchestration without the wait:

```bash
ARXIV_MAX_PAPERS=300 python day4/lesson.py
```

## Run It

```bash
python day4/lesson.py
```

The same workflow also runs from here:

```bash
python day4/day4_orchestrated_workflow.py
```

By default it runs once and stops.

## What It Does

1. Runs the Day 2 ingestion pipeline.
2. Creates the Day 3 query views and materialized views.
3. Runs the Day 3 export and benchmark.
4. Writes a report to `day4/output/`.

You will see the views get created twice in the logs, once by Day 4's own step and once by the Day 3 script it calls. That is expected and harmless, since the SQL drops and recreates them anyway. Day 3 creates its own views so that it works when you run it on its own.

## Watching A Failure Recover

This is the part worth doing in class.

Set the sabotage target to a step, and that step fails the first time it runs:

```env
DAY4_SABOTAGE_TARGET=day3_views
DAY4_SABOTAGE_MODE=once
```

Run the workflow and watch the logs:

```text
Task run 'Provision Day 3 views' - Task run failed with exception:
  RuntimeError('Intentional Day 4 sabotage triggered for stage: day3_views')
  - Retry 1/2 will start 5 second(s) from now
Task run 'Provision Day 3 views' - Day 3 views provisioned
Flow run - Finished in state Completed()
```

It failed, waited five seconds, tried again, worked, and the workflow carried on. That is the entire lesson of Day 4 in five lines of log.

`DAY4_SABOTAGE_MODE=once` means it only fails the first time. The pipeline drops a small marker file in `day4/output/` to remember it has already sabotaged that step. Set the mode to `always` and it fails every attempt, so you can watch the retries run out and the workflow give up. That is worth seeing too.

The targets you can sabotage are `day2_pipeline`, `day3_views`, `day3_export`, and `workflow_checkpoint`. The first three trigger a task retry. `workflow_checkpoint` triggers the outer workflow retry, so you can see the difference between the two.

Set `DAY4_SABOTAGE_TARGET=off` when you are done.

## Watching It In The Prefect UI

Prefect has a local web page, which makes the whole thing much less abstract. Start it in a second terminal:

```bash
prefect server start
```

Then open the address it prints, usually `http://127.0.0.1:4200`, and run the Day 4 script from the same shell.

Worth looking at:

- The flow run page for the Day 4 workflow.
- The three task cards, and their states.
- The retry attempts when sabotage is on. Seeing a red attempt followed by a green one lands better than reading about it.
- The logs panel.

## Running It On A Schedule

Everything so far runs once when you type the command. Real pipelines run on their own.

```env
DAY4_SCHEDULE_ENABLED=true
DAY4_SCHEDULE_EVERY_HOURS=24
DAY4_SCHEDULE_EVERY_DAYS=0
DAY4_SCHEDULE_NAME=day4-orchestrated-every-24h
```

Then start it the same way:

```bash
python day4/day4_orchestrated_workflow.py
```

Instead of running once and exiting, the process stays alive and triggers the workflow on the interval you set. If `DAY4_SCHEDULE_EVERY_DAYS` is above 0 it wins, otherwise the hours value is used.

Leave scheduling until the one-shot run makes sense. A schedule is just a repeated trigger for the workflow you already have.

## Settings

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

## What The Files Are

- `lesson.py`: the short version. Read this first.
- `pipeline/`: config, Prefect tasks, the workflow and its retry loop, reporting, scheduling, and the sabotage hook.
- `day4_orchestrated_workflow.py`: a second way to run the same workflow.
- `DAY4_CODE_WALKTHROUGH.md`: the slower explanation.
- `output/`: the orchestration report and the sabotage marker files.

## What To Look At Afterwards

- The Prefect logs, especially any retry attempts.
- The Day 4 report in `day4/output/`.
- The Day 2 and Day 3 outputs, which the workflow refreshed as it ran.

## A Note For Teaching

Day 4 shows three ideas that turn scripts into a system: steps run in a deliberate order, failures get retried close to where they happened, and a bigger failure can restart the whole thing. None of these are exotic, and all of them are the difference between something that works when you are watching and something that works when you are not.
