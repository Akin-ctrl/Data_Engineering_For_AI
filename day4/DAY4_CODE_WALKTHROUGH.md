# Day 4 Code Walkthrough

Day 4 has been refactored so the first file you open in class is small and readable.

Start here:

- `day4/lesson.py`

Use the files inside `day4/pipeline/` only when you want to explain one specific implementation detail.

## Big Picture

Day 4 teaches orchestration: running earlier pipeline steps in the right order with retries and reporting.

```text
Run Day 2 ingestion -> create Day 3 views -> run Day 3 export -> write Day 4 report
```

The important learning idea is:

> A workflow is more than several scripts. It is an ordered recovery plan.

## What Changes From Day 3

Day 3 ran one benchmark pipeline.

Day 4 coordinates multiple steps:

- run the Day 2 API ingestion
- provision SQL views
- run the Day 3 export/benchmark
- optionally demonstrate failure and retry behavior
- write an orchestration report
- optionally serve a Prefect schedule

The pipeline shape is:

```text
configure -> validate files -> run workflow -> write report
```

## File Map

| File | What It Explains |
|---|---|
| `day4/lesson.py` | The complete orchestration flow in readable order. |
| `day4/day4_orchestrated_workflow.py` | Compatibility wrapper so the original run command still works. |
| `day4/pipeline/constants.py` | Shared paths, defaults, and allowed sabotage settings. |
| `day4/pipeline/config.py` | Environment variables and typed runtime config. |
| `day4/pipeline/logging_utils.py` | JSON logging setup. |
| `day4/pipeline/models.py` | Step and report dataclasses. |
| `day4/pipeline/paths.py` | Fast checks that required scripts exist. |
| `day4/pipeline/subprocess_utils.py` | Child process runner for Day 2, SQL, and Day 3. |
| `day4/pipeline/sabotage.py` | Intentional failure hook for retry demos. |
| `day4/pipeline/tasks.py` | Prefect task definitions. |
| `day4/pipeline/workflow.py` | Prefect flow and outer workflow retry loop. |
| `day4/pipeline/report.py` | Day 4 report and database snapshot. |
| `day4/pipeline/schedule.py` | Optional Prefect schedule serving. |

## Walkthrough Order For Class

### 1. Open `day4/lesson.py`

Show the orchestration in this order:

```text
check required files exist
load config
if scheduled mode is enabled, serve schedule
otherwise run workflow with retries
write orchestration report
```

Do not open the Prefect task file first.

### 2. Explain The Workflow Shape

Open `day4/pipeline/workflow.py`.

Focus on:

- `day4_flow`
- `run_with_workflow_retries`

The teaching point:

> The workflow defines the order, while the retry wrapper defines recovery.

### 3. Explain The Tasks

Open `day4/pipeline/tasks.py`.

Focus on the three tasks:

- `run_day2_pipeline`
- `provision_day3_views`
- `run_day3_export`

The teaching point:

> Each task should represent one meaningful pipeline step.

### 4. Explain Subprocess Execution

Open `day4/pipeline/subprocess_utils.py`.

Students only need to understand:

- Day 4 reuses earlier scripts
- each child command gets the project environment
- failures bubble up through `check=True`

The teaching point:

> Orchestration often coordinates existing tools instead of rewriting them.

### 5. Explain Sabotage And Retries

Open `day4/pipeline/sabotage.py`.

Focus on:

- `DAY4_SABOTAGE_TARGET`
- `DAY4_SABOTAGE_MODE`
- marker file for one-time failure

The teaching point:

> Controlled failure makes retry behavior visible and less magical.

### 6. Explain Reporting

Open `day4/pipeline/report.py`.

Focus on:

- step results
- Day 3 report summary
- database row counts

The teaching point:

> A workflow should leave behind evidence of what happened.

### 7. Explain Scheduling Last

Open `day4/pipeline/schedule.py` only after the one-shot workflow is clear.

The teaching point:

> Scheduling is just another way to trigger the same workflow repeatedly.

## What To Skip On First Pass

Skip these until learners ask:

- every Prefect decorator option
- every report field
- every database snapshot query
- every schedule serving detail
- exact subprocess command construction
- every environment variable

Those details are useful after students understand orchestration order.

## Common Student Questions

### Why does Day 4 run Day 2 and Day 3 instead of copying their code?

Because orchestration should coordinate reusable units, not duplicate logic.

### Why are there task retries and workflow retries?

Task retries handle local failures. Workflow retries handle larger boundary failures.

### Why include sabotage?

Because students learn retries faster when they can intentionally trigger a failure and watch recovery happen.

### Why write a Day 4 report?

Because logs scroll away. A report gives the workflow a durable summary.

### Why is scheduling optional?

Because learners should understand one successful run before learning repeated scheduled runs.

## Instructor Script

Use this short explanation:

> Day 4 turns earlier scripts into a workflow. It runs ingestion first, creates query views second, runs exports third, and writes a report at the end. The retry demo shows how orchestration helps a pipeline recover from temporary failures.

That is the Day 4 lesson.
