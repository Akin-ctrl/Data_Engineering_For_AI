# Student Onboarding Guide
## Data Engineering for AI — Module Setup

This guide walks you through every installation step needed to run this module from scratch.  
**Assumed starting point:** A Windows computer with only a browser installed.

---

## What You Are Installing and Why

| Component | Technology | Why You Need It |
|---|---|---|
| **Runtime** | Python 3.11 | Runs all pipeline scripts |
| **Editor** | VS Code | Where you read and run the code |
| **Version Control** | Git | Clone the repository to your machine |
| **Database** | PostgreSQL 16 via Docker Compose | Stores raw, clean, and rejected records |
| **Container Engine** | Docker Desktop | Runs PostgreSQL without a manual install |
| **ORM / Query Layer** | SQLAlchemy + psycopg | Python talks to PostgreSQL through these |
| **Environment Config** | python-dotenv (.env file) | Keeps credentials out of your code |
| **Data Libraries** | pandas, pyarrow, requests | Load, transform, and export data |
| **Logging** | Custom JsonFormatter (already in code) | No install needed — it is pure Python |

---

## Part 1 — Install Python 3.11

Python is the language every script in this module is written in.

### Step 1.1 — Download the installer

Go to: **https://www.python.org/downloads/**

Click **"Download Python 3.11.x"** (the latest 3.11 release shown).

> [!IMPORTANT]
> Do **not** install Python 3.12 or 3.13 — some dependencies in this module have not caught up yet. Stick to **3.11**.

### Step 1.2 — Run the installer

1. Open the downloaded `.exe` file.
2. On the first screen, **tick the checkbox** that says:  
   ✅ `Add Python 3.11 to PATH`
3. Click **"Install Now"**.
4. Wait for it to finish, then click **"Close"**.

### Step 1.3 — Verify the install

Open **Command Prompt** (press `Win + R`, type `cmd`, press Enter) and run:

```cmd
python --version
```

Expected output:
```
Python 3.11.x
```

Also verify pip (Python's package installer) is present:

```cmd
pip --version
```

Expected output (version number may differ):
```
pip 24.x.x from C:\Users\...\Python311\Lib\site-packages\pip (python 3.11)
```

> [!TIP]
> If `python` is not recognized, close Command Prompt, reopen it, and try again. If it still fails, rerun the installer and make sure **"Add Python to PATH"** was ticked.

---

## Part 2 — Install Git

Git lets you download (clone) this repository and track changes.

### Step 2.1 — Download Git for Windows

Go to: **https://git-scm.com/download/win**

The download starts automatically. Run the installer.

### Step 2.2 — Run the installer

Accept all defaults by clicking **"Next"** through every screen.  
The important settings are already correct by default.

### Step 2.3 — Verify

Open a **new** Command Prompt window and run:

```cmd
git --version
```

Expected output:
```
git version 2.x.x.windows.x
```

---

## Part 3 — Install VS Code

VS Code is your code editor. It is free, lightweight, and has excellent Python support.

### Step 3.1 — Download VS Code

Go to: **https://code.visualstudio.com/**

Click **"Download for Windows"**. Run the installer, accept defaults.

### Step 3.2 — Install the Python extension

1. Open VS Code.
2. Press `Ctrl + Shift + X` to open the Extensions panel.
3. Search for **"Python"**.
4. Install the extension published by **Microsoft** (it is the first result).
5. Also install **"Pylance"** — same search, same publisher.

---

## Part 4 — Install Docker Desktop

Docker Desktop runs PostgreSQL inside a container.  
This means you **do not** need to install PostgreSQL manually — Docker handles it.

### Step 4.1 — Download Docker Desktop

Go to: **https://www.docker.com/products/docker-desktop/**

Click **"Download for Windows"**. Run the installer.

### Step 4.2 — Enable WSL 2 when prompted

During installation, Docker will ask you to enable **WSL 2** (Windows Subsystem for Linux).  
Click **"OK"** or **"Install"** whenever prompted. Your computer may restart.

> [!NOTE]
> WSL 2 is a lightweight Linux environment built into Windows. Docker needs it to run containers efficiently. You do not need to use the Linux terminal yourself.

### Step 4.3 — Start Docker Desktop

After installation, open **Docker Desktop** from the Start menu.  
Wait until you see a green status indicator that says **"Docker is running"** (bottom left corner).

> [!IMPORTANT]
> Docker Desktop must be **running** every time you work on this module. It is not a background service by default — you need to open it.

### Step 4.4 — Verify Docker

Open Command Prompt and run:

```cmd
docker --version
```

Expected output:
```
Docker version 26.x.x, build ...
```

Also verify Docker Compose:

```cmd
docker compose version
```

Expected output:
```
Docker Compose version v2.x.x
```

---

## Part 5 — Clone the Repository

### Step 5.1 — Open a terminal in VS Code

1. Open VS Code.
2. Press `` Ctrl + ` `` to open the integrated terminal.

### Step 5.2 — Navigate to where you want the project

```cmd
cd C:\Users\YourName\Desktop
```

(Replace `YourName` with your actual Windows username.)

### Step 5.3 — Clone the repo

```cmd
git clone <your-repo-url> Data_Engineering_For_AI
cd Data_Engineering_For_AI
```

> [!NOTE]
> Your instructor will give you the exact repository URL. If you are working from a folder already provided, skip this step and simply open the folder in VS Code (`File → Open Folder`).

---

## Part 6 — Create a Python Virtual Environment

A virtual environment keeps this project's dependencies isolated from the rest of your system.  
**Always create and activate the virtual environment before doing anything else.**

### Step 6.1 — Create the environment

Inside VS Code's terminal, from the project root folder:

```cmd
python -m venv .venv
```

This creates a `.venv` folder inside the project. It contains a private Python installation.

### Step 6.2 — Activate the environment

```cmd
.venv\bin\activate
```

Your terminal prompt will change to show `(.venv)` at the start:

```
(.venv) C:\Users\YourName\Desktop\Data_Engineering_For_AI>
```

> [!IMPORTANT]
> You must run this activation command **every time** you open a new terminal window. The virtual environment is not active automatically.

### Step 6.3 — Tell VS Code to use this environment

1. Press `Ctrl + Shift + P`.
2. Type **"Python: Select Interpreter"** and press Enter.
3. Choose the option that shows `.venv` in the path.

---

## Part 7 — Install Python Dependencies

With the virtual environment active, install all required packages:

```cmd
pip install -r requirements.txt
```

This installs:

| Package | What it does |
|---|---|
| `pandas` | Loads and transforms tabular data (CSV, DataFrames) |
| `pyarrow` | Reads and writes Parquet files; fast columnar data engine |
| `sqlalchemy` | Python ORM — writes SQL queries and manages connections |
| `psycopg[binary]` | PostgreSQL database driver (SQLAlchemy uses this to connect) |
| `python-dotenv` | Reads your `.env` file and loads credentials as environment variables |
| `requests` | Makes HTTP requests — used to download CSVs and call the ArXiv API |

### Verify the install

```cmd
pip list
```

You should see all six packages in the list with their versions.

---

## Part 8 — Configure the Environment File

Your credentials and connection settings live in a `.env` file.  
The pipeline scripts read from this file automatically using `python-dotenv`.

### Step 8.1 — Copy the example file

```cmd
copy .env.example .env
```

### Step 8.2 — Open and edit `.env`

Open `.env` in VS Code. Fill in the values:

```env
# ── Day 1: HuggingFace CSV source ──────────────────────────────────────────
HF_CSV_URL=https://huggingface.co/datasets/Censius-AI/ECommerce-Women-Clothing-Reviews/resolve/main/Womens%20Clothing%20E-Commerce%20Reviews.csv

# ── PostgreSQL connection ───────────────────────────────────────────────────
PGHOST=localhost
PGPORT=5432
PGDATABASE=de_ai
PGUSER=de_user
PGPASSWORD=change_me           # ← change this to any password you like

# ── Day 2: ArXiv API ────────────────────────────────────────────────────────
ARXIV_BASE_URL=https://export.arxiv.org/api/query
ARXIV_SOURCE_NAME=arxiv_multi_domain
ARXIV_MAX_PAPERS=10000
ARXIV_PAGE_SIZE=100
ARXIV_SLEEP_SECONDS=5
ARXIV_DEFAULT_LOOKBACK_DAYS=180
ARXIV_OVERLAP_MINUTES=10
ARXIV_REQUEST_TIMEOUT_SECONDS=90
ARXIV_REQUEST_MAX_RETRIES=6
ARXIV_RETRY_BACKOFF_SECONDS=5
ARXIV_USER_AGENT=DataEngineeringForAI-Day2/1.0 (contact: your@email.com)

# ── Day 3: Benchmark settings ───────────────────────────────────────────────
DAY3_SOURCE_TABLE=training_data.clean_papers
DAY3_ORDER_BY=paper_key
DAY3_OUTPUT_DIR=day3/output
DAY3_EXPORT_BASENAME=day3_clean_papers_benchmark
DAY3_BENCHMARK_RUNS=10
```

> [!WARNING]
> Never commit your `.env` file to Git. The `.gitignore` in this project already excludes it. Keep your credentials local.

---

## Part 9 — Start PostgreSQL with Docker Compose

This starts a PostgreSQL 16 database running inside a Docker container.

### Step 9.1 — Make sure Docker Desktop is open and running

Look for the green **"Docker is running"** label in Docker Desktop before proceeding.

### Step 9.2 — Start the database

From the project root in your terminal (with `.venv` active):

```cmd
docker compose up -d
```

- `-d` means "detached" — it runs in the background.
- Docker downloads the PostgreSQL 16 image on first run (this takes ~1 minute).

### Step 9.3 — Check that it is healthy

```cmd
docker compose ps
```

Expected output:

```
NAME              IMAGE                COMMAND                  STATUS
de_ai_postgres    postgres:16-alpine   "docker-entrypoint.s…"   Up (healthy)
```

The **STATUS** must say `Up (healthy)` before you run any pipeline scripts.

> [!TIP]
> If the status shows `Up (health: starting)`, wait 15 seconds and run `docker compose ps` again.

### Step 9.4 — Stop the database when you are done

```cmd
docker compose down
```

Your data is preserved in a Docker volume (`pgdata`) and will be there next time you run `docker compose up -d`.

---

## Part 10 — Verify the Full Stack

Run this quick check to confirm Python → dotenv → SQLAlchemy → psycopg → PostgreSQL all connect end-to-end:

```cmd
python -c "
from dotenv import load_dotenv
import os
from sqlalchemy import create_engine, text

load_dotenv()

url = (
    f'postgresql+psycopg://{os.environ[\"PGUSER\"]}:{os.environ[\"PGPASSWORD\"]}'
    f'@{os.environ[\"PGHOST\"]}:{os.environ[\"PGPORT\"]}/{os.environ[\"PGDATABASE\"]}'
)

engine = create_engine(url)
with engine.begin() as conn:
    result = conn.execute(text('SELECT version()')).scalar_one()
    print('Connected:', result)
"
```

Expected output:
```
Connected: PostgreSQL 16.x on x86_64-pc-linux-musl, ...
```

---

## Part 11 — Run the Labs

Once setup is complete, run each day's script from the project root:

```cmd
# Day 1 — Download HuggingFace CSV and load into PostgreSQL
python day1/day1_hf_csv_to_postgres.py

# Day 2 — Pull ArXiv papers via API and load into PostgreSQL
python day2/day2_arxiv_api_to_postgres.py

# Day 3 — Export clean data to CSV and Parquet, run benchmark
python day3/day3_postgres_to_csv_parquet_benchmark.py
```

> [!NOTE]
> Day 2 fetches up to 10,000 papers and respects ArXiv's rate limit (5-second sleep between pages). It may take 15–30 minutes to complete fully. This is normal — the pipeline is being polite to the API.

---

## Troubleshooting

| Problem | Likely Cause | Fix |
|---|---|---|
| `python` not recognized | PATH not set | Reinstall Python with "Add to PATH" ticked |
| `pip` not recognized | Same as above | Same fix |
| `(.venv)` not showing | Virtual env not activated | Run `.venv\Scripts\activate` again |
| `docker: command not found` | Docker Desktop not started | Open Docker Desktop and wait for green status |
| `connection refused` on port 5432 | PostgreSQL container not running | Run `docker compose up -d` |
| `(healthy)` not showing | Container still starting | Wait 15s, re-run `docker compose ps` |
| `Missing required environment variables` | `.env` file missing or incomplete | Re-do Part 8 |
| `ModuleNotFoundError` | Dependencies not installed | Re-run `pip install -r requirements.txt` with `.venv` active |
| `psycopg` import error | Wrong psycopg version | Run `pip install "psycopg[binary]>=3.2.0"` |

---

## Quick Reference Cheatsheet

```cmd
# Every session — do these first:
docker compose up -d          # start the database
.venv\Scripts\activate        # activate Python environment

# Run a lab script:
python day1/day1_hf_csv_to_postgres.py

# Stop the database when done:
docker compose down

# Check container status anytime:
docker compose ps

# Check installed packages:
pip list
```

---

## Setup Verification Checklist

Before your first lab session, tick each item:

- [ ] `python --version` returns `3.11.x`
- [ ] `pip --version` returns a version number
- [ ] `git --version` returns a version number
- [ ] Docker Desktop shows green **"Docker is running"**
- [ ] `docker --version` returns a version number
- [ ] `docker compose version` returns a version number
- [ ] Virtual environment created and shows `(.venv)` in terminal
- [ ] `pip install -r requirements.txt` completed without errors
- [ ] `.env` file created and `PGPASSWORD` changed from `change_me`
- [ ] `docker compose up -d` shows container as `Up (healthy)`
- [ ] Full stack verify command prints `Connected: PostgreSQL 16.x ...`
