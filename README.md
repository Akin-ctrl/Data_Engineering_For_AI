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

## Instructor Notes

- Keep labs small enough to complete within session time.
- Emphasize reasoning and tradeoffs, not only final code.
- Use corrections as a learning checkpoint, not a shortcut.

## Next Step in the 10-Week Journey

After this 2-week Data Engineering module, learners continue into the remaining weeks toward Agentic AI with stronger data fundamentals and better pipeline thinking.
