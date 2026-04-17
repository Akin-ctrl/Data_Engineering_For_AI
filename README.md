# Data Engineering for AI (2-Week Module)

## Overview

This repository contains the lab materials for the **Data Engineering** module of a **10-week training program leading to Agentic AI**.

The full program spans 10 weeks, and this module covers **2 weeks** focused on the data foundations required before building reliable AI and agentic systems.

This repo is designed to support:

- Guided lab sessions during class.
- Learner self-practice after class.
- Reference corrections when learners need to verify their approach.

## Module Position in the 10-Week Program

- Total training duration: 10 weeks.
- This module duration: 2 weeks.
- Purpose of this module: build practical data engineering skills that feed directly into later Agentic AI topics.

## Learning Objectives (2 Weeks)

By the end of this module, learners should be able to:

- Explain the role of data engineering in AI/Agentic AI systems.
- Build and reason about basic ingestion and transformation workflows.
- Identify and fix common data quality issues.
- Produce clean, structured, reusable datasets for downstream AI tasks.


## Repository Purpose

This repository hosts lab work so learners can:

- Follow step-by-step exercises in class.
- Revisit lab instructions after class.
- Compare with correction/reference solutions when needed.

Use this repo as a practical companion to the live sessions, not as a replacement for active lab work.

## Day 1 Lab Entry Point

The first practical lab is ready and focused on the foundational data engineering flow:

- Download raw CSV from HuggingFace.
- Load into pandas memory.
- Clean data defensively.
- Upsert safe records into PostgreSQL.
- Persist rejected rows for review.

Start here:

- `day1/README.md`
- `day1/day1_hf_csv_to_postgres.py`
- `.env.example`
- `docker-compose.yml`

## Day 2 Lab Entry Point

The second practical lab moves from a static CSV to a live public API:

- Query ArXiv for cs.LG papers.
- Paginate through the Atom feed with a simple persisted watermark.
- Flatten repeated authors into a normalized child table.
- Load raw, clean, and rejected records idempotently into PostgreSQL.

Start here:

- `day2/README.md`
- `day2/day2_arxiv_api_to_postgres.py`

## Day 3 Lab Entry Point

The third practical lab turns the clean PostgreSQL data into file formats that are easy to compare:

- Query the Day 2 clean table from PostgreSQL.
- Export the same data to CSV and Parquet.
- Measure read speed with pandas and PyArrow.
- Compare file sizes and produce an "aha" benchmark report.

Start here:

- [day3/README.md](day3/README.md)
- [day3/day3_postgres_to_csv_parquet_benchmark.py](day3/day3_postgres_to_csv_parquet_benchmark.py)

## Day 4 Lab Entry Point

The fourth practical lab wraps the Day 2 and Day 3 code into one orchestrated workflow with retries:

- Run the API extract first.
- Load PostgreSQL second.
- Provision the Day 3 query views.
- Generate the CSV and Parquet benchmark outputs last.
- Inject a controlled failure and watch the retry behavior recover.

Start here:

- [day4/README.md](day4/README.md)
- [day4/day4_orchestrated_workflow.py](day4/day4_orchestrated_workflow.py)

## How Learners Should Use This Repo

1. Start from the lab instructions first.
2. Attempt the exercise independently.
3. Validate your output against expected checks.
4. Only then consult correction/reference material.
5. Document what you learned from differences.

## Expectations for Lab Corrections

Correction/reference materials should:

- Show one clear and reproducible approach.
- Explain key design choices briefly.
- Include basic validation logic where relevant.
- Stay aligned with concepts taught in class.

## Next Step in the 10-Week Journey

After this 2-week Data Engineering module, learners continue into the remaining weeks toward Agentic AI with stronger data fundamentals and better pipeline thinking.
