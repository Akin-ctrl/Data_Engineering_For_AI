# Day 4 Code Walkthrough

The slower version of the Day 4 README. Open `day4/lesson.py` first, and go into `day4/pipeline/` only when you want to see how one piece works.

If you have not read the "Words You Will Need" section in [README.md](README.md), read that first. This file assumes you know what a task, a flow, and a retry are.

## The Shape Of The Day

```text
run Day 2 ingestion -> create Day 3 views -> run Day 3 export -> write a report
```

Every one of those steps already existed. Day 4 adds nothing new to what the pipeline does. What it adds is what happens when a step fails.

> A workflow is not just several scripts in a row. It is an ordered plan that includes what to do when something breaks.

That distinction is the day. Three scripts run by hand work fine until the second one fails at 3am and the third one runs anyway, on stale data, and nobody notices for a week.

## What Changed Since Day 3

Day 3 was one pipeline. Day 4 coordinates several:

- run the Day 2 ingestion
- create the SQL views
- run the Day 3 export and benchmark
- optionally break something on purpose to watch recovery
- write a report about what happened
- optionally keep running on a schedule

```text
configure -> check the files it needs exist -> run the workflow -> write the report
```

## Where Everything Lives

| File | What It Does |
|---|---|
| `day4/lesson.py` | The whole orchestration in readable order. |
| `day4/day4_orchestrated_workflow.py` | A second way to run the same workflow. |
| `day4/pipeline/constants.py` | Paths, defaults, and the list of valid sabotage targets. |
| `day4/pipeline/config.py` | Reads `.env` and checks the settings. |
| `day4/pipeline/logging_utils.py` | Sets up the JSON log output. |
| `day4/pipeline/models.py` | Small dataclasses for step results and the report. |
| `day4/pipeline/paths.py` | Checks the scripts it needs are actually there, before starting. |
| `day4/pipeline/subprocess_utils.py` | Runs the Day 2 and Day 3 scripts as child processes. |
| `day4/pipeline/sabotage.py` | The deliberate failure hook for the retry demo. |
| `day4/pipeline/tasks.py` | The three Prefect tasks. |
| `day4/pipeline/workflow.py` | The Prefect flow and the outer retry loop. |
| `day4/pipeline/report.py` | Writes the report and a snapshot of the database. |
| `day4/pipeline/schedule.py` | Optional scheduled running. |

## Going Through It In Class

### 1. Start with `day4/lesson.py`

```text
check the required files exist
load config
if scheduled mode is on, serve the schedule
otherwise run the workflow with retries
write the orchestration report
```

Notice the first line. Before anything else, it checks that the scripts it is about to run actually exist. That check takes no time and turns a confusing failure fifteen minutes in into an obvious one at the start. Failing early and clearly is a habit worth pointing at.

Do not open the Prefect task file yet.

### 2. The Workflow

Open `day4/pipeline/workflow.py`.

Two things:

- `day4_flow` is the Prefect flow. It says which tasks run and in what order.
- `run_with_workflow_retries` wraps the whole flow in the outer retry loop.

Keeping these separate is the point. The flow describes the order. The wrapper describes recovery. Mixing them into one function makes both harder to follow.

> The workflow says what order things happen in. The retry wrapper says what happens when that goes wrong.

### 3. The Tasks

Open `day4/pipeline/tasks.py`.

Three tasks, one per meaningful step:

- `run_day2_pipeline`
- `provision_day3_views`
- `run_day3_export`

Look at the decorator above each one. `retries` and `retry_delay_seconds` are all it takes to get retry behaviour, which is most of why a library like Prefect is worth using. Writing that yourself is not hard, but it is fiddly and easy to get subtly wrong.

Each task returns a small result object recording what ran and when. That is what the report is built from later.

`provision_day3_views` is worth a note. It calls the same `provision_views` function Day 3 uses, rather than having its own copy. There is one place where view provisioning happens, and both days use it.

> One task should be one meaningful step, not one line of code and not the entire pipeline.

### 4. Running The Other Days

Open `day4/pipeline/subprocess_utils.py`.

Day 4 runs Day 2 and Day 3 as separate programs, the same way you would from a terminal. It passes the project environment through, and `check=True` means a failing child process raises an error rather than being ignored.

That `check=True` is small and important. Without it, a child script could fail and the workflow would carry on cheerfully to the next step.

Running them as child processes rather than importing them keeps each day independently runnable, which matters when the whole point is that a student can run any day on its own.

> Orchestration usually coordinates tools that already exist, rather than rewriting them.

### 5. Sabotage And Retries

Open `day4/pipeline/sabotage.py`. This file exists only for teaching, and it is small enough to read in one go.

Every task calls `maybe_trigger_sabotage` before doing its real work. If the configured target matches that step, it raises an error on purpose.

`DAY4_SABOTAGE_MODE` decides how often:

- `once` fails the first attempt, then writes a marker file so the retry succeeds. This is the one to use in class, because you see failure followed by recovery.
- `always` fails every attempt, so you can watch the retries run out and the workflow finally give up. Also worth showing.

The marker file lives in `day4/output/` and is just a small flag on disk. Delete it to run the demo again.

> Retries are much easier to understand when you can cause a failure yourself and watch what happens.

### 6. The Report

Open `day4/pipeline/report.py`.

It records what each step did, pulls in the Day 3 benchmark summary, and takes a snapshot of the database row counts. All of it goes to a JSON file in `day4/output/`.

The reason is simple: terminal output scrolls away, and the next run overwrites what you remember seeing. A workflow that runs unattended needs to leave evidence behind.

> A workflow should leave a record of what it did, because nobody is watching it at 3am.

### 7. Scheduling, Last

Open `day4/pipeline/schedule.py` only once a single run makes sense to everyone.

When schedule mode is on, the process stays alive and triggers the same workflow on an interval instead of running once and exiting.

> Scheduling is nothing new. It is another way to trigger the workflow you already have.

## What To Skip The First Time

Leave these until someone asks:

- every Prefect decorator option
- every field in the report
- the database snapshot queries
- the details of how the schedule is served
- exactly how the subprocess commands are built
- every environment variable

## Questions Students Ask

### Why run Day 2 and Day 3 instead of copying their code into Day 4?

Because two copies of anything drift apart. Fix a bug in Day 2 and you would have to remember to fix it in Day 4 too. Orchestration coordinates things that already exist.

### Why both task retries and workflow retries?

Task retries handle small local failures cheaply, without re-running everything. Workflow retries handle the failures that leave things in a state a single step cannot recover from. Most of the time the task retry is enough.

### Why deliberately break the pipeline?

Because reading about retries teaches almost nothing. Watching a step fail, wait, and succeed teaches it in about ten seconds.

### Why write a report when the logs already say all this?

Because logs scroll away, and a scheduled run happens when you are not there. The report is the durable version.

### Why is scheduling optional?

Because you should understand one successful run before adding a repeating trigger to it.

### Why do the views get created twice?

Day 4 creates them, then the Day 3 script it runs creates them again. It is harmless, since the SQL drops and recreates them, and it is the price of Day 3 still working when you run it on its own. Worth pointing out so nobody thinks it is a bug.

## The Short Version

> Day 4 turns yesterday's scripts into a workflow. Ingestion runs first, the views are created second, the export runs third, and a report gets written at the end. When a step fails it is retried where it failed, and if that is not enough the whole workflow can start over. The sabotage switch lets you cause a failure on purpose and watch the recovery happen.
