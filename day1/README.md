# Day 1 Lab: HuggingFace CSV to PostgreSQL

## What You Are Building

You are going to download a spreadsheet of clothing reviews from the internet, look at every row to decide whether you trust it, and put the good rows into a database while keeping the bad ones on the side so you can see what went wrong.

That last part is the whole point of Day 1. Real data is messy. A row with a rating of 9 out of 5, or a review with no text in it, will break whatever you build later. The job is to catch those now, and to keep them rather than quietly throw them away, so you can go back and look at what your source data is actually doing.

The dataset:

- `Censius-AI/ECommerce-Women-Clothing-Reviews`
- One split, `train`, about 23,500 rows
- The file itself is `Womens Clothing E-Commerce Reviews.csv`

## Before You Start

You need PostgreSQL running and your Python packages installed. If you have not set your machine up yet, do that first in [STUDENT_ONBOARDING.md](../STUDENT_ONBOARDING.md), then come back here.

Once your machine is ready, two things every time:

1. Copy `.env.example` to `.env` and fill in your values, if you have not already.
2. Start the database:

```bash
docker compose up -d
```

## Run It

```bash
python day1/lesson.py
```

Open `lesson.py` first. It is short, and it shows the whole story of the day in one screen. The detailed work lives in `day1/pipeline/`, and you only need to open those files when you want to see how one particular step is done.

There is a second way to run it, which does exactly the same thing:

```bash
python day1/day1_hf_csv_to_postgres.py
```

## What The Files Are

- `lesson.py`: the short version you open in class. Read this one first.
- `pipeline/`: the actual work, split into small files for config, extract, transform, load, checks, and outputs.
- `day1_hf_csv_to_postgres.py`: a second way to run the same pipeline.
- `DAY1_CODE_WALKTHROUGH.md`: a slower explanation of each part.
- `output/`: where the downloaded file and the rejected rows end up.

## What Ends Up In The Database

The pipeline creates its own schema and three tables. A schema is just a named folder inside the database that keeps a set of tables together.

- `de_ai.raw_reviews`: every row exactly as it arrived, before anyone judged it.
- `de_ai.clean_reviews`: the rows that passed every check.
- `de_ai.rejected_reviews`: the rows that failed, with a note saying why.

Three tables instead of one is deliberate. If you only keep the clean rows, you have no way of answering "how much did we throw away, and was that right?" Keeping the raw copy means you can rebuild the clean table later with different rules, without downloading anything again.

### Why Day 1 Uses A Different Schema

Day 1 lives in the `de_ai` schema. Days 2 to 5 use a schema called `training_data` instead.

That is on purpose. Day 1 is a standalone lab about cleaning a downloaded CSV, and nothing later depends on it. Days 2 to 5 are one connected pipeline: Day 2 loads the papers, Day 3 exports and benchmarks them, Day 4 runs the whole thing as one workflow, and Day 5 turns the text into training data. Those four share a schema because they share a dataset.

So when you get to Day 2 and your Day 1 tables look like they have disappeared, they have not. They are still sitting in `de_ai`, and the new work is going into `training_data`.

The table names for Day 1 are all in `day1/pipeline/constants.py` if you want to see them in one place.

## The Checks The Pipeline Runs

Every row has to get past all of these. A row that fails even one goes to the rejected table, along with the reason it failed.

- The file has the columns we expect. If the source changes shape, we stop rather than guess.
- Column names are tidied up. `Positive Feedback Count` becomes `positive_feedback_count`, because spaces and capitals are painful to work with in SQL.
- Number columns are actually turned into numbers. A rating that arrives as the text `"four"` becomes empty rather than silently breaking a later calculation.
- The rating is between 1 and 5.
- `recommended_ind` is 0 or 1, since it is a yes or no flag.
- The feedback count is not negative.
- The review actually has text in it.
- The age, when present, is between 13 and 100.

A row can fail more than one check, and the rejected table records all of the reasons, not just the first one.

Two other things worth knowing:

- Every row keeps a full copy of its original values, so you can always see what arrived.
- Loading uses an upsert, which means "insert this row, or update it if it is already there". That is what lets you run the pipeline twice without ending up with two copies of everything.

## What To Look At Afterwards

- The JSON log lines in your terminal, especially the final counts of raw, clean and rejected rows.
- `day1/output/rejected_sample_<batch_id>.csv`, which is a sample of what got thrown out. Open it. Seeing the actual bad rows is more useful than reading about them.
- The row counts in the three database tables.
- The rating distribution printed near the end, which tells you whether the clean data still looks sensible.

## A Note For Teaching

Day 1 is deliberately one straight-line script. There are no classes, no command line options, and no configuration beyond the `.env` file, because the point is to see the shape of a pipeline without anything else in the way.

Later days add the pieces this one leaves out: a live API instead of a file, orchestration, retries, and file formats built for analytics.
