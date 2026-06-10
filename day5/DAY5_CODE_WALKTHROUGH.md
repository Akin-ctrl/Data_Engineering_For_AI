# Day 5 Code Walkthrough

Day 5 has been refactored so the first file you open in class is small and readable.

Start here:

- `day5/lesson.py`

Use the files inside `day5/pipeline/` only when you want to explain one specific implementation detail.

## Big Picture

Day 5 teaches how clean text becomes model-ready instruction payloads.

```text
Clean papers -> sentence chunks -> instruction records -> validation -> train/validation JSONL
```

The important learning idea is:

> AI training data still needs data engineering: structure, validation, deterministic splits, and manifests.

## What Changes From Earlier Days

Days 1 to 4 produced clean, queryable data assets.

Day 5 turns those assets into supervised examples:

- read clean paper text
- split text into chunks
- generate instruction/input/output records
- generate chat-style records
- validate payload shape and length
- split records into train and validation sets
- write JSONL files and a manifest

The pipeline shape is:

```text
configure -> read source -> chunk text -> build tasks -> validate -> split -> export
```

## File Map

| File | What It Explains |
|---|---|
| `day5/lesson.py` | The complete payload flow in readable order. |
| `day5/day5_build_instruction_payload.py` | Compatibility wrapper so the original run command still works. |
| `day5/pipeline/constants.py` | Defaults and identifier validation patterns. |
| `day5/pipeline/config.py` | Environment variables and typed runtime config. |
| `day5/pipeline/logging_utils.py` | JSON logging setup. |
| `day5/pipeline/models.py` | Chunk, Alpaca, and chat dataclasses. |
| `day5/pipeline/source.py` | PostgreSQL source query and DataFrame loading. |
| `day5/pipeline/text.py` | Text normalization, sentence splitting, chunking, and chunk records. |
| `day5/pipeline/tasks.py` | Summary, classification, keypoint, Alpaca, and chat builders. |
| `day5/pipeline/validation.py` | Strict payload validation. |
| `day5/pipeline/split.py` | Deterministic train/validation split. |
| `day5/pipeline/outputs.py` | JSONL payload writing and manifest creation. |

## Walkthrough Order For Class

### 1. Open `day5/lesson.py`

Show the payload pipeline in this order:

```text
load config
load source papers
build chunks
build Alpaca records
build chat records
validate records
split train/validation ids
write JSONL payloads
write manifest
```

Do not open the chunking algorithm first.

### 2. Explain The Source

Open `day5/pipeline/source.py`.

Students only need to understand:

- the source is `training_data.clean_papers`
- each row provides title, summary, category, and id
- optional `DAY5_MAX_PAPERS` can limit the run

The teaching point:

> Day 5 starts from clean data; it should not re-solve ingestion.

### 3. Explain Chunking

Open `day5/pipeline/text.py`.

Focus on:

- `normalize_whitespace`
- `split_sentences`
- `chunk_text`
- `build_chunk_records`

The teaching point:

> Large source text needs stable, bounded chunks before it can become training examples.

### 4. Explain Task Generation

Open `day5/pipeline/tasks.py`.

Focus on the three generated task types:

- summarize
- classify
- keypoints

Then show the two output formats:

- Alpaca-style records
- chat-style records

The teaching point:

> One source chunk can produce multiple supervised examples.

### 5. Explain Validation

Open `day5/pipeline/validation.py`.

Focus on:

- duplicate ids
- empty instruction/input/output fields
- minimum input length
- minimum output length
- chat message shape

The teaching point:

> Payload files should fail fast before training if the examples are malformed.

### 6. Explain Splitting

Open `day5/pipeline/split.py`.

Focus on:

- fixed random seed
- train/validation ratio
- non-empty split checks

The teaching point:

> Reproducible splits make model experiments easier to compare.

### 7. Explain Outputs

Open `day5/pipeline/outputs.py`.

Focus on:

- JSONL writers
- all/train/validation files
- manifest metadata

The teaching point:

> A good training-data pipeline writes both payloads and metadata about how they were made.

## What To Skip On First Pass

Skip these until learners ask:

- every regex detail
- every dataclass field
- every manifest key
- every output path property
- the exact hashing logic
- every validation branch

Those details are useful after students understand the payload-building story.

## Important Teaching Caveat

The generated summaries and keypoints are deterministic and extractive. They are useful for teaching data-shaping mechanics, but they are not a gold-standard production fine-tuning dataset.

Say this plainly:

> Day 5 teaches payload construction, not perfect annotation quality.

## Common Student Questions

### Why create chunks?

Because source documents can be too long or inconsistent. Chunks give each example a bounded input.

### Why generate multiple tasks from one chunk?

Because the same source text can teach different behaviors: summarization, classification, and extraction.

### Why validate before writing?

Because broken training records are much cheaper to fix before they reach a model job.

### Why write both Alpaca and chat formats?

Because different fine-tuning tools expect different payload shapes.

### Why write a manifest?

Because the manifest records how the dataset was produced, which makes experiments easier to reproduce.

## Instructor Script

Use this short explanation:

> Day 5 takes the clean paper text from earlier labs and turns it into supervised training examples. We chunk each paper, generate tasks from each chunk, validate the records, split them into train and validation sets, and write JSONL files plus a manifest.

That is the Day 5 lesson.
