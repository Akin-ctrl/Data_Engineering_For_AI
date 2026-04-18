# Day 5 Lab: Build Instruction-Tuning Payloads From Pipeline Text

## Goal

Take the cleaned research text collected in earlier days, chunk it with a sentence-based overlap strategy, transform each chunk into strict instruction-tuning pairs, and export the final AI payload files.

This day turns data engineering outputs into model-ready supervised fine-tuning data.

## What This Day Covers

- Reading source text from `training_data.clean_papers`.
- Sentence-based chunking with controlled overlap.
- Generating deterministic instruction pairs for three tasks:
  - summarize
  - classify
  - keypoints
- Exporting dual payload formats:
  - Alpaca-style (`instruction`, `input`, `output`)
  - Chat-style (`messages`)
- Applying strict validation checks before export.
- Writing deterministic train/validation splits.

## Files

- `day5_build_instruction_payload.py`: Day 5 payload generation pipeline.
- `README.md`: Day 5 quickstart and runbook.
- `DAY5_CODE_WALKTHROUGH.md`: detailed section-by-section explanation.
- `output/`: generated payload and manifest artifacts.

## Install Dependencies

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Run the Day 5 Pipeline

```bash
python day5/day5_build_instruction_payload.py
```

## Default Inputs and Outputs

Default source table:

- `training_data.clean_papers`

Default output directory:

- `day5/output`

Default generated files:

- `day5/output/day5_instruction_payload_alpaca_all.jsonl`
- `day5/output/day5_instruction_payload_alpaca_train.jsonl`
- `day5/output/day5_instruction_payload_alpaca_val.jsonl`
- `day5/output/day5_instruction_payload_chat_all.jsonl`
- `day5/output/day5_instruction_payload_chat_train.jsonl`
- `day5/output/day5_instruction_payload_chat_val.jsonl`
- `day5/output/day5_instruction_payload_manifest.json`

## Environment Variables

Recommended Day 5 settings:

```env
DAY5_SOURCE_TABLE=training_data.clean_papers
DAY5_ORDER_BY=paper_key
DAY5_OUTPUT_DIR=day5/output
DAY5_EXPORT_BASENAME=day5_instruction_payload
DAY5_MAX_CHUNK_WORDS=350
DAY5_CHUNK_OVERLAP_WORDS=50
DAY5_MIN_INPUT_WORDS=30
DAY5_MIN_OUTPUT_WORDS=3
DAY5_TRAIN_RATIO=0.9
DAY5_SPLIT_SEED=42
DAY5_MAX_PAPERS=0
```

## Strict Validation Rules

The script fails fast if any of the following is violated:

- Required keys are missing.
- `instruction`, `input`, or `output` is empty.
- Input word count is below `DAY5_MIN_INPUT_WORDS`.
- Output word count is below `DAY5_MIN_OUTPUT_WORDS` for summarize/keypoints.
- Classification output is enforced as non-empty and may be one token.
- Duplicate record IDs are detected.
- Chat format does not have exactly one `user` and one `assistant` message.

## Train/Validation Split Behavior

- Split is deterministic and seed-controlled.
- Default is 90/10 train/validation.
- The same seed produces the same split IDs.

## What To Inspect After Run

- Console JSON logs for source row count, chunk count, and payload count.
- The generated `*_manifest.json` file for run metadata.
- One or two JSONL records from each output file to verify schema shape.

## Teaching Note

Day 5 is where the class sees that feature-ready tables are not enough for LLM fine-tuning. You still need a disciplined transformation layer that turns source text into validated supervision examples.
