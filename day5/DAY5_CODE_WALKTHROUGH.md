# Day 5 Code Walkthrough

The slower version of the Day 5 README. Open `day5/lesson.py` first, and go into `day5/pipeline/` only when you want to see how one step works.

## The Shape Of The Day

```text
clean papers -> chunks -> instruction records -> validation -> train and validation files
```

Days 1 to 4 built clean data you can query. Day 5 turns that into something a model can be trained on, and it turns out that is still a data engineering job rather than a machine learning one.

> Training data needs the same discipline as any other data: a defined structure, validation before you trust it, a split you can reproduce, and a record of how it was made.

## Words You Will Need

**Instruction tuning.** Teaching a model to follow instructions by showing it thousands of examples of an instruction, some input, and the correct response. Day 5 builds those examples.

**Alpaca format.** One common shape for those examples: three fields called `instruction`, `input`, and `output`. Named after the Alpaca project that popularised it.

**Chat format.** A different shape for the same thing, written as a list of messages with roles, one from the user and one from the assistant. Different training tools want different shapes, so we write both.

**JSONL.** A text file where each line is one complete JSON object. Not a JSON array, just one object per line. It is the normal format for training data because you can read it one line at a time without loading the whole file.

**Chunk.** A piece of a longer document. Models have a limit on how much text they can take at once, so long documents get cut into chunks.

**Overlap.** Each chunk repeats the last few words of the one before it. Without overlap, a sentence that happens to land on a boundary gets cut in half and both pieces become nonsense.

**Train and validation split.** You keep some examples back from training so you can test the model on things it has not seen. Usually 90% train, 10% validation.

**Manifest.** A small file recording how the dataset was built: how many records, what settings were used, when it ran. Six months later this is how you work out what you actually trained on.

## Where Everything Lives

| File | What It Does |
|---|---|
| `day5/lesson.py` | The whole pipeline in readable order. |
| `day5/day5_build_instruction_payload.py` | A second way to run the same pipeline. |
| `day5/pipeline/constants.py` | The defaults, including the chunk window size. |
| `day5/pipeline/config.py` | Reads `.env` and checks the settings. |
| `day5/pipeline/logging_utils.py` | Sets up the JSON log output. |
| `day5/pipeline/models.py` | Small dataclasses for chunks and the two record formats. |
| `day5/pipeline/source.py` | The query that reads the clean papers. |
| `day5/pipeline/text.py` | Whitespace tidying, sentence splitting, chunking. |
| `day5/pipeline/tasks.py` | Builds the summarize, classify and keypoints examples. |
| `day5/pipeline/validation.py` | Checks every record before anything is written. |
| `day5/pipeline/split.py` | The repeatable train and validation split. |
| `day5/pipeline/outputs.py` | Writes the JSONL files and the manifest. |

## Going Through It In Class

### 1. Start with `day5/lesson.py`

```text
load config
load the source papers
build chunks
build Alpaca records
build chat records
validate everything
split into train and validation ids
write the JSONL files
write the manifest
```

Do not open the chunking code first. See the shape before the machinery.

### 2. The Source

Open `day5/pipeline/source.py`.

It reads `training_data.clean_papers`, the table Day 2 filled, taking the id, title, summary and category from each row. `DAY5_MAX_PAPERS` can cap the number of papers when you want a quick run.

> Day 5 starts from clean data. It should not be solving ingestion problems again.

### 3. Chunking

Open `day5/pipeline/text.py`.

Four functions to look at:

- `normalize_whitespace` collapses runs of spaces and newlines, so chunk boundaries are predictable.
- `split_sentences` breaks text at sentence endings, because cutting mid-sentence produces garbage examples.
- `chunk_text` assembles sentences into chunks up to the size limit, carrying the overlap between them.
- `build_chunk_records` runs that across every paper and gives each chunk an id.

Notice that chunking works sentence by sentence rather than counting words blindly. A chunk that ends halfway through a thought teaches the model nothing useful.

### 4. Why The Window Is Only 60 Words

This is worth stopping on, because the number looks wrong.

`DEFAULT_MAX_CHUNK_WORDS` is 60. A real project working with full documents would use 350 or more. So why the small number here?

Because of what we are reading. ArXiv abstracts are short. Across the roughly 11,800 papers in the corpus, the average is about 180 words and the longest is 306. At a 350 word window, not one abstract in the entire dataset would ever be split. Every chunk would be a whole abstract, the overlap setting would never once be used, and the entire lesson would be invisible.

At 60 words each abstract splits into about five chunks, and you can put two next to each other and see the repeated words where they overlap. Try it: open one of the output files and compare the end of one chunk with the start of the next.

The overlap is 15 words, a quarter of the window. That ratio is the usual choice. Set the overlap close to the window size, say 50 out of 60, and consecutive chunks become near-copies of each other, which wastes space and teaches the model the same sentence over and over.

If you run this pipeline over full paper text one day, raise the window to 350 and the overlap to around 50. Nothing in the code changes, only the numbers.

There is a test guarding this, in `tests/test_day5_text.py`. If someone raises the window to a size where abstract-length text stops splitting, that test fails and says why.

### 5. Building The Examples

Open `day5/pipeline/tasks.py`.

Each chunk produces three different examples:

- **summarize**: given the chunk, produce a summary
- **classify**: given the chunk, name its category
- **keypoints**: given the chunk, pull out the key points

Then each of those is written in both Alpaca and chat format.

So one chunk becomes three examples, in two formats. That is how roughly 11,800 papers turn into about 63,000 chunks and then 189,000 records.

> The same piece of source text can teach several different behaviours, so one chunk is worth more than one training example.

### 6. Validation

Open `day5/pipeline/validation.py`.

Every record is checked before anything is written:

- no duplicate ids
- no empty instruction, input or output
- the input is long enough to be worth training on
- the output is long enough for summarize and keypoints
- chat records have exactly one user message and one assistant message

If anything fails, the pipeline stops and writes nothing.

That last part is deliberate. Half a good dataset is worse than none, because you will not notice, and you will train on it. Fixing a broken record here costs a minute. Finding it after a training run costs the training run.

> Bad training data is much cheaper to catch before it reaches the model than after.

### 7. Splitting

Open `day5/pipeline/split.py`. It is short, and the important part is one line: the random shuffle uses a fixed seed.

That means the same input always produces the same split. Run it today and next week and the same records land in validation both times.

If the split were random each run, you could not compare two experiments. Any difference in results might be the model, or might just be that this run happened to get an easier validation set. A fixed seed removes that question.

It also refuses to produce an empty train or validation set, which is the failure you get by accident with a ratio of 1.0 or a tiny dataset.

> A split you cannot reproduce makes every experiment you run afterwards impossible to compare.

### 8. Outputs

Open `day5/pipeline/outputs.py`.

It writes the JSONL files, all records plus the train and validation splits, in both formats. Then it writes the manifest.

The manifest is the piece people skip and later wish they had. It records how many records, what settings produced them, and when. When you have four experiments and one of them worked, that file is how you find out what was different about it.

> Write the data and write down how you made it. The second part is what makes the first part reusable.

## What To Skip The First Time

Leave these until someone asks:

- the regex details in sentence splitting
- every dataclass field
- every manifest key
- how the output paths are built
- the chunk id hashing
- every branch of the validation

## An Honest Caveat

The summaries and keypoints this generates are produced by rules, not by a model. They take the first sentences, pull out phrases, and so on. They are deterministic and repeatable, which is exactly what you want for teaching the mechanics.

They are not a good fine-tuning dataset. Real instruction data is written or reviewed by people, or generated by a strong model and then checked.

Say this plainly in class:

> Day 5 teaches how to build the payloads. It does not teach how to write good training examples, and the two are different problems.

## Questions Students Ask

### Why chunk at all?

Because documents vary wildly in length and models have limits. Chunking gives every example a bounded, predictable size.

### Why the overlap?

Because a sentence that lands on a chunk boundary gets cut in half otherwise, and both halves are useless. The overlap means the whole sentence appears intact in at least one chunk.

### Why three tasks from one chunk?

Because the same text can teach summarising, classifying, and extracting. Throwing away two of those would be wasteful.

### Why both Alpaca and chat formats?

Because different fine-tuning tools expect different shapes, and converting later is more annoying than writing both now.

### Why validate before writing instead of after?

Because a file that exists looks finished. If validation runs first and the pipeline refuses to write, there is no half-good dataset lying around waiting to be used by mistake.

### Why does the manifest matter?

Because in three months you will have several datasets and no memory of what was different about them.

## The Short Version

> Day 5 takes the clean paper text from the earlier days and turns it into training examples. We cut each paper into overlapping chunks, build three kinds of example from each chunk in two different formats, check every record before writing anything, split them into train and validation with a fixed seed so the split is repeatable, and write the files along with a manifest saying how they were made.
