# Data Engineering for AI

This repository holds the lab work for the Data Engineering module of a 10 week programme that leads into Agentic AI. This module is 2 weeks of it, and it covers the data foundations you need before building anything that relies on data being right.

## What This Module Is About

Every AI system is downstream of some data. If that data is wrong, incomplete, or quietly different from what you thought it was, nothing you build on top of it will save you. This module is about making data trustworthy, and about knowing when it is not.

By the end of it you should be able to:

- Explain why data engineering decides whether an AI system works.
- Build a pipeline that pulls data in, cleans it, and stores it.
- Spot the ways data goes wrong, and handle them on purpose rather than by accident.
- Produce clean, structured datasets that something downstream can actually use.

## How The Week Fits Together

The five days build on each other. Each one adds a problem the previous day did not have.

| Day | What you build | The new problem |
|---|---|---|
| 1 | Download a CSV, clean it, load it into PostgreSQL | Data arrives broken, and you have to decide what to do with the broken parts |
| 2 | Pull papers from a live API into PostgreSQL | The source keeps changing, comes in pages, and is nested rather than tabular |
| 3 | Export to CSV and Parquet, and measure the difference | File formats are not interchangeable, and the reason is not what most people assume |
| 4 | Run Days 2 and 3 as one workflow with retries | Things fail, and something has to handle that when you are not watching |
| 5 | Turn the text into instruction-tuning data | Training data still needs structure, validation, and reproducibility |

Days 2 to 5 form one connected pipeline and share a database schema. Day 1 stands alone.

## Getting Set Up

If your machine is not ready yet, start here:

- [STUDENT_ONBOARDING.md](STUDENT_ONBOARDING.md) covers Python, Git, VS Code, Docker, and getting the database running.
- [WSL_Docker_Portainer_Setup.md](WSL_Docker_Portainer_Setup.md) is for Windows machines using Ubuntu under WSL2.

Once that is done, every lab needs the same two things: the database running (`docker compose up -d`) and your `.env` file filled in.

## The Labs

Each day has a README with the quickstart and a walkthrough with the slower explanation. Start with the README.

**[Day 1](day1/README.md)** ([walkthrough](day1/DAY1_CODE_WALKTHROUGH.md)). Download a CSV of clothing reviews, check every row, load the good ones into PostgreSQL and keep the bad ones with a note saying what was wrong.

**[Day 2](day2/README.md)** ([walkthrough](day2/DAY2_CODE_WALKTHROUGH.md)). Pull research papers from the ArXiv API, a page at a time, parse the XML, split authors into their own table, and remember where you stopped so tomorrow's run carries on.

**[Day 3](day3/README.md)** ([walkthrough](day3/DAY3_CODE_WALKTHROUGH.md)). Export the clean papers to CSV and Parquet, create SQL views inside the database, and time how long each format takes to read back. The answer is more interesting than expected.

**[Day 4](day4/README.md)** ([walkthrough](day4/DAY4_CODE_WALKTHROUGH.md)). Run Day 2 and Day 3 as a single workflow with retries, then break a step on purpose and watch it recover.

**[Day 5](day5/README.md)** ([walkthrough](day5/DAY5_CODE_WALKTHROUGH.md)). Turn the paper text into instruction-tuning examples, validate every one, split them repeatably, and write the files with a manifest.

## How To Use This Repository

Try the exercise yourself before reading the reference code. That order matters more than it sounds. Reading a working solution feels like understanding, and it usually is not.

1. Read the lab instructions.
2. Attempt it on your own.
3. Check your output against the checks the lab describes.
4. Only then read the reference code here.
5. Write down where yours differed and why.

## Running The Tests

There is a small test suite covering the parts that are easy to get subtly wrong: column name cleanup, row hashing, the clean and rejected split, chunking, and the train and validation split.

```bash
pytest tests/ -q
```

Worth running, and worth reading. Tests are also documentation of what the code is supposed to do.

## Timing

Day 2 takes 12 to 15 minutes for a full run, because it waits 5 seconds between API requests to be polite to a free public service. Day 4 runs Day 2, so it takes about as long.

For a quick run while you are working on something else, cap the paper count:

```bash
ARXIV_MAX_PAPERS=300 python day2/lesson.py
```

Days 1, 3 and 5 are all under a minute.

## What Comes Next

After these 2 weeks the programme moves on toward Agentic AI. The habits from this module carry over directly. An agent making decisions from bad data makes bad decisions faster than anything else you will build.
