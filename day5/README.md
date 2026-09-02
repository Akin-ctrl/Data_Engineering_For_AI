# Day 5 Lab: Build Instruction-Tuning Payloads

## What You Are Building

Four days of work have left you with a clean, queryable table of research papers. That is a good data asset, and it is not training data.

Day 5 closes that gap. You take the paper text, cut it into pieces, turn each piece into examples of the form "here is an instruction, here is some input, here is the correct answer", check every one of them, split them into a training set and a test set, and write the files a fine-tuning tool would actually accept.

The point of the day is that this is still a data engineering job. Structure, validation, a repeatable split, and a record of what you did. None of it is machine learning, and all of it decides whether the training run is worth anything.

## Words You Will Need

**Instruction tuning.** Teaching a model to follow instructions by showing it many examples of an instruction, some input, and the right response.

**Alpaca format.** A common shape for those examples: `instruction`, `input`, `output`. Named after the project that popularised it.

**Chat format.** The same information written as a list of messages with roles, one from the user and one from the assistant. Different tools want different shapes, so we produce both.

**JSONL.** A file where every line is one complete JSON object. Not an array, one object per line. It is standard for training data because you can stream it a line at a time.

**Chunk.** A piece of a longer document. Models limit how much text they take at once, so long text gets cut up.

**Overlap.** Each chunk repeats the last few words of the previous one, so a sentence landing on a boundary is not sliced in half.

**Train and validation split.** Hold some examples back from training so you can test on things the model has not seen. Usually 90/10.

**Manifest.** A small file recording how the dataset was built. It is what tells you, months later, what you actually trained on.

## Before You Start

Day 5 reads the table Day 2 filled, so Day 2 has to have run.

```bash
docker compose up -d
```

## Run It

```bash
python day5/lesson.py
```

The same pipeline also runs from here:

```bash
python day5/day5_build_instruction_payload.py
```

It is fast, well under a minute, because all the slow work happened on earlier days.

## What It Produces

From roughly 11,800 papers you get about 63,000 chunks and about 189,000 records. Each chunk becomes three kinds of example, in two formats.

Files land in `day5/output/`:

- `day5_instruction_payload_alpaca_all.jsonl`
- `day5_instruction_payload_alpaca_train.jsonl`
- `day5_instruction_payload_alpaca_val.jsonl`
- `day5_instruction_payload_chat_all.jsonl`
- `day5_instruction_payload_chat_train.jsonl`
- `day5_instruction_payload_chat_val.jsonl`
- `day5_instruction_payload_manifest.json`

The three kinds of example are **summarize** (write a summary of this chunk), **classify** (what category is this), and **keypoints** (pull out the main points).

## Why The Chunk Window Is Only 60 Words

`DAY5_MAX_CHUNK_WORDS` is set to 60, which looks small next to the 350 a real project would use. That is deliberate, and it is worth understanding.

The reason is the source text. ArXiv abstracts are short. Across the whole corpus the average is about 180 words and the longest is 306. Set the window to 350 and not one abstract in the entire dataset would ever be split. Every chunk would be a whole abstract, the overlap setting would never be used once, and you would never see the thing this lesson exists to show.

At 60 words each abstract splits into about five chunks, and the overlap becomes visible. Open one of the output files, find two consecutive chunks from the same paper, and compare the end of one against the start of the next. The last 15 words of one are the first 15 of the other.

The overlap is 15 words, a quarter of the window, which is the usual ratio. Push it close to the window size, say 50 out of 60, and consecutive chunks turn into near-copies of each other. That wastes space and teaches the model the same sentence repeatedly.

If you point this pipeline at full paper text instead of abstracts, raise the window to 350 and the overlap to around 50. The code does not change, only the numbers.

## The Checks Before Anything Is Written

The pipeline refuses to write a single file if any record fails:

- no duplicate ids
- `instruction`, `input` and `output` all have content
- the input is at least `DAY5_MIN_INPUT_WORDS` long
- the output is at least `DAY5_MIN_OUTPUT_WORDS` long for summarize and keypoints
- classification output is present, and may be a single word
- chat records have exactly one user message and one assistant message

All or nothing is the right behaviour here. Half a dataset is worse than no dataset, because you will not notice and you will train on it.

## The Split Is Repeatable

The train and validation split uses a fixed seed, so the same input always produces the same split. Default is 90% train, 10% validation.

That matters more than it sounds. If the split changed every run, you could never tell whether a better result came from your change or from a luckier validation set.

## Settings

```env
DAY5_SOURCE_TABLE=training_data.clean_papers
DAY5_ORDER_BY=paper_key
DAY5_OUTPUT_DIR=day5/output
DAY5_EXPORT_BASENAME=day5_instruction_payload
DAY5_MAX_CHUNK_WORDS=60
DAY5_CHUNK_OVERLAP_WORDS=15
DAY5_MIN_INPUT_WORDS=30
DAY5_MIN_OUTPUT_WORDS=3
DAY5_TRAIN_RATIO=0.9
DAY5_SPLIT_SEED=42
DAY5_MAX_PAPERS=0
```

`DAY5_MAX_PAPERS=0` means no limit. Set it to a small number for a quick run.

## What The Files Are

- `lesson.py`: the short version. Read this first.
- `pipeline/`: config, source loading, chunking, task building, validation, splitting, and output writing.
- `day5_build_instruction_payload.py`: a second way to run the same pipeline.
- `DAY5_CODE_WALKTHROUGH.md`: the slower explanation.
- `output/`: the JSONL files and the manifest.

## What To Look At Afterwards

- The log line at the end with the source, chunk and record counts.
- The manifest, which records the settings that produced this dataset.
- A couple of actual records from each file. Open them. Reading two real examples explains the format faster than any description.
- Two consecutive chunks from the same paper, to see the overlap.

## An Honest Caveat

The summaries and keypoints here are generated by rules, not by a model. They take leading sentences and pull out phrases. That makes them deterministic and repeatable, which is exactly right for teaching the mechanics.

It does not make them good training data. Real instruction datasets are written or reviewed by people, or generated by a strong model and then checked by people.

Worth saying plainly in class: Day 5 teaches how to build the payloads, not how to write good examples. Those are two different problems, and only the first one is data engineering.

## A Note For Teaching

This is the day the week pays off. Day 1 taught rejecting bad rows. Day 2 taught handling a live source. Day 3 taught choosing a file format. Day 4 taught running it all reliably. Day 5 shows what it was for, and shows that the discipline does not stop when the word "AI" appears.
