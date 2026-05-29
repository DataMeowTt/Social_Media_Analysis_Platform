# Social Media Analysis Platform

An end-to-end data pipeline for ingesting, processing, and analyzing social media content from Twitter, YouTube, and Facebook. Raw data moves through a medallion architecture (Bronze → Silver → Gold) on AWS S3, orchestrated by Apache Airflow and processed by Apache Spark. A FastAPI backend and React dashboard expose pipeline status and embedded Superset analytics.

---

## Features

- **Multi-platform ingestion** — Twitter (twitterapi.io, multi-key rotation), YouTube (Data API v3), Facebook (browser automation)
- **Medallion storage** — Raw JSON.gz (Bronze), processed Parquet/Snappy (Silver), analytics-ready Parquet (Gold) on AWS S3; schema registered automatically in AWS Glue Data Catalog
- **PySpark processing** — cleaning, enrichment, schema validation, and deduplication distributed across a Spark cluster
- **ML inference** — Vietnamese sentiment analysis via Google Gemini API; stance detection (agreement / disagreement / neutral) via a local zero-shot NLI model (HuggingFace Transformers, offline)
- **Conversation threading** — groups YouTube comments into parent-reply threads enriched with sentiment and stance signals, then runs Gemini analysis per thread
- **Airflow orchestration** — DAG-per-platform with retry logic; Spark jobs dispatched via `docker compose run`
- **Dashboard** — React SPA with live pipeline status (polls every 30 s, 10 s during active runs), task-level progress view, embedded Superset charts, and Facebook controversial-post insights
- **Token-based auth** — Bearer token gate in front of all dashboard API endpoints

---

## Tech Stack

| Layer | Technology |
|---|---|
| Language | Python 3.11 |
| Processing | Apache Spark 3.5.0 (PySpark) |
| Orchestration | Apache Airflow 3.2.0 |
| Cloud Storage | AWS S3 (s3a via hadoop-aws 3.3.4) |
| Schema Catalog | AWS Glue Data Catalog |
| Data Warehouse | AWS Redshift |
| ML Inference | Google Gemini 2.5 Flash Lite, HuggingFace Transformers |
| Backend API | FastAPI + Uvicorn |
| Frontend | React 18, Vite, Tailwind CSS |
| Visualization | Apache Superset (embedded via guest token) |
| Airflow Metadata DB | PostgreSQL 16 |
| Superset Cache | Redis 7 |
| Containerization | Docker Compose |
| Linting | Ruff |
| Testing | pytest, pytest-asyncio |
| CI | GitHub Actions |

---

## Architecture Overview

### Data Flow

```
External APIs / Browser Scraping
        │
        ▼
  [Ingestion Job]  (Python, async)
        │  JSON.gz chunks, 300 MB buffer
        ▼
  S3 Bronze Layer   raw/{platform}/{year}/{month}/{day}/
        │
        ▼
  [Processing Job]  (PySpark — Spark cluster)
        │  clean · enrich · validate · deduplicate
        ▼
  S3 Silver Layer   silver/{table}/{year}/{month}/{day}/
        │
        ├─────────────────────────────────────────┐
        ▼                                         ▼
  [Analytics Job]  (PySpark)         [ML Inference Jobs]  (PySpark + Gemini / local model)
        │                                         │
        ▼                                         ▼
  S3 Gold Layer   analytics/{platform}/   analytics/{platform}/sentiment/  stance/
        │
        ▼
  AWS Glue Data Catalog  (auto-registered on every write)
        │
        ├── AWS Redshift  (optional)
        │
        └── Apache Superset ←→ FastAPI Backend ←→ React Dashboard
```

### Key Design Decisions

- **Spark dispatched by Airflow via Docker** — `BashOperator` runs `docker compose run --rm spark-submit`, keeping Airflow lightweight and Spark fully containerized. Cluster mode (`spark://spark-master:7077`) for CPU-bound processing; local mode (`local[*]`) for ML inference jobs that need model state in a single process.
- **Buffer-flush ingestion** — accumulates records in a 300 MB in-memory buffer before flushing to S3, preventing thousands of tiny files on large fetches.
- **Multi-key Twitter rotation** — `MY_API_KEY_1`, `MY_API_KEY_2`, … are cycled automatically when a key's credits are exhausted.
- **Glue Catalog auto-registration** — `write_to_S3()` creates/updates the Glue table and partition metadata on every write; no manual `MSCK REPAIR TABLE` needed.
- **Offline ML models** — Transformers models are baked into `Dockerfile.pipeline` at build time. `TRANSFORMERS_OFFLINE=1` and `HF_HUB_OFFLINE=1` are set in all Spark executor environments.
- **Conversation threading threshold** — only threads with more than 50 replies are promoted to Gemini analysis, controlling API cost.

---

## Folder Structure

```
.
├── .github/workflows/
│   ├── ci.yml                  # Lint + test (pure-Python and Spark, parallel)
│   └── cd.yml                  # Placeholder — not implemented
│
├── app/                        # Dashboard application
│   ├── frontend/               # React + Vite SPA
│   │   └── src/
│   │       ├── components/     # StatusBanner, TaskFlow, RunChart, RunTable, SupersetEmbed, …
│   │       ├── pages/          # LoginPage
│   │       ├── App.jsx         # Root layout, polling logic, platform/view routing
│   │       └── utils.js        # apiFetch, formatDate
│   ├── main.py                 # FastAPI app — auth, pipeline status, Superset proxy, S3 insights
│   ├── airflow_client.py       # Async Airflow API v2 client
│   ├── superset_client.py      # Superset guest-token provider
│   ├── s3_insights.py          # Reads YouTube thread insights from S3
│   └── fb_insights.py          # Reads Facebook controversial posts from S3
│
├── configs/
│   ├── base.yaml               # Shared config (Twitter API URL)
│   ├── dev.yaml                # Dev S3 bucket
│   ├── test.yaml               # Test S3 bucket
│   └── prod.yaml               # Production S3 bucket
│
├── fb_temporary/               # Standalone Facebook scraping scripts (outside main pipeline)
│
├── infra/docker/
│   ├── docker-compose.yaml     # Full stack: Airflow, Spark, Superset, PostgreSQL, Redis
│   ├── Dockerfile.airflow      # Airflow image customization
│   └── Dockerfile.pipeline     # Spark worker image + bundled ML models
│
├── src/
│   ├── twitter/
│   │   ├── ingestion/          # Async twitterapi.io client, fetcher, multi-key rotation
│   │   ├── processing/         # PySpark: clean, enrich, normalize, validate
│   │   └── analytics/          # PySpark: aggregate, enrich, dimensions/facts schemas
│   │
│   ├── youtube/
│   │   ├── ingestion/          # YouTube Data API v3 client + fetcher
│   │   ├── processing/         # PySpark: clean, enrich, validate
│   │   ├── analytics/          # PySpark: sentiment/stance aggregations
│   │   └── conversation/       # Thread assembly + Gemini analysis + S3 output
│   │
│   ├── facebook/
│   │   ├── ingestion/          # Browser-based scraper (group posts + comments)
│   │   ├── processing/         # PySpark processing
│   │   └── analytics/          # PySpark analytics + Gemini conversation analysis
│   │
│   ├── ml/
│   │   └── inference/
│   │       ├── gemini_sentiment.py   # Gemini API batch sentiment (Vietnamese)
│   │       ├── youtube_sentiment.py  # Local HuggingFace sentiment model
│   │       ├── youtube_stance.py     # Zero-shot NLI stance detection
│   │       ├── batch_inference.py    # Batch inference runner
│   │       └── predictor.py          # Model loader abstraction
│   │
│   ├── storage/
│   │   ├── s3/
│   │   │   ├── uploader.py     # upload_to_bronze_s3(), write_to_S3() + Glue registration
│   │   │   ├── reader.py       # Spark-based S3 reads
│   │   │   └── partitioning.py # S3 key generation
│   │   └── redshift/
│   │       └── loader.py       # Redshift load utility
│   │
│   ├── orchestration/
│   │   ├── airflow/dags/
│   │   │   ├── twitter/        # twitter_pipeline, twitter_pipeline_test DAGs
│   │   │   ├── youtube/        # youtube_pipeline, youtube_gemini_analysis DAGs
│   │   │   ├── facebook/       # facebook_pipeline DAG
│   │   │   └── _spark.py       # spark_cmd() / local_spark_cmd() builders
│   │   └── jobs/               # Entry-point scripts called by each DAG task
│   │       ├── twitter/
│   │       ├── youtube/
│   │       └── facebook/
│   │
│   └── utils/
│       ├── config_loader.py    # ENV-driven YAML config loader (lru_cache)
│       ├── logger.py           # Structured logger
│       ├── session.py          # boto3 S3/Glue client factory, Spark session factory
│       ├── reader.py           # Generic S3/local data reader
│       ├── helpers.py          # Miscellaneous utilities
│       └── deleter_duplicate.py
│
└── tests/
    ├── conftest.py             # Shared SparkSession fixture (scope=session)
    ├── test_ingestion.py       # Twitter ingestion — pure Python
    ├── test_processing.py      # Twitter processing — Spark
    ├── test_analytics.py       # Twitter analytics — Spark
    ├── test_yt_processing.py   # YouTube processing — Spark
    ├── test_fb_processing.py   # Facebook processing — Spark
    ├── test_fb_analytics.py    # Facebook analytics — Spark
    ├── test_fb_conversation.py # Facebook conversation — pure Python
    ├── test_utils.py           # Utility tests — pure Python
    └── test_ai/                # Standalone model evaluation scripts (excluded from CI)
```

---

## Prerequisites

| Requirement | Version |
|---|---|
| Python | 3.11 |
| Java | 17 (local Spark / CI) |
| Docker + Docker Compose | Engine 24+ |
| Node.js | 18+ (frontend dev only) |

---

## Installation

```bash
git clone <repo-url>
cd Social_Media_Analysis_Platform

# Python dependencies
pip install -r requirements.txt

# PySpark (required for Spark jobs and Spark tests)
pip install pyspark==3.5.0

# Frontend
cd app/frontend && npm install
```

---

## Environment Variables

Copy `.env.example` to `.env` and populate all required values. The file is loaded by both the Python application and Docker Compose.

```bash
cp .env.example .env
```

| Variable | Required | Default | Description |
|---|---|---|---|
| `ENV` | Yes | — | `dev`, `test`, or `prod`. Selects `configs/{ENV}.yaml`. |
| `MY_API_KEY_1` | Yes* | — | twitterapi.io key. Add `MY_API_KEY_2`, `MY_API_KEY_3`, … for rotation. |
| `YOUTUBE_API_KEY` | Yes* | — | Google YouTube Data API v3 key. |
| `GEMINI_API_KEY` | Yes* | — | Google Gemini API key. |
| `MODEL` | No | `gemini-2.5-flash-lite` | Gemini model ID. |
| `AWS_ACCESS_KEY_ID` | Yes | — | AWS credentials. |
| `AWS_SECRET_ACCESS_KEY` | Yes | — | AWS credentials. |
| `AWS_DEFAULT_REGION` | No | `ap-southeast-1` | AWS region. |
| `HOST_WORKSPACE` | Yes | — | Absolute path to repo root on the host (mounted into Spark containers). |
| `AIRFLOW_UID` | No | `50000` | UID for Airflow containers. |
| `AIRFLOW_URL` | No | `http://localhost:8080` | Airflow API base URL (used by dashboard backend). |
| `AIRFLOW_USER` | No | `admin` | Airflow login username. |
| `AIRFLOW_PASSWORD` | No | `admin` | Airflow login password. |
| `USERNAME` | No | `admin` | Dashboard login username. |
| `PASSWORD` | No | `admin` | Dashboard login password. |
| `SUPERSET_URL` | No | `http://localhost:8088` | Superset base URL. |
| `SUPERSET_ADMIN_USER` | No | `admin` | Superset admin username. |
| `SUPERSET_ADMIN_PASS` | No | `admin` | Superset admin password. |
| `SUPERSET_DASHBOARDS_TWITTER` | No | — | Comma-separated `id:label` pairs for Twitter Superset dashboards. |
| `SUPERSET_DASHBOARDS_YOUTUBE` | No | — | Comma-separated `id:label` pairs for YouTube Superset dashboards. |
| `SUPERSET_DASHBOARDS_FACEBOOK` | No | — | Comma-separated `id:label` pairs for Facebook Superset dashboards. |
| `POSTGRES_USER` | No | `airflow` | PostgreSQL user for the Airflow metadata database. |

*Required only for the respective platform.

---

## Running Locally

### 1. Start the infrastructure stack

```bash
cd infra/docker
docker compose up -d
```

| Service | URL |
|---|---|
| Airflow UI | http://localhost:8080 |
| Spark Master UI | http://localhost:8090 |
| Superset | http://localhost:8088 |

### 2. Start the dashboard backend

```bash
cd app
pip install fastapi uvicorn httpx python-dotenv
python main.py
# Listening on http://localhost:8000
```

### 3. Start the frontend dev server

```bash
cd app/frontend
npm run dev
# http://localhost:5173  (proxies /api → localhost:8000)
```

### 4. Trigger a pipeline run

Use the Airflow UI to trigger any DAG, or run ingestion directly from the command line:

```bash
# Twitter
QUERY="your search query" ENV=dev python -m src.orchestration.jobs.twitter.ingestion_job

# YouTube
ENV=dev python -c "
from src.orchestration.jobs.youtube.ingestion_job import run_ingestion_task
run_ingestion_task()
"
```

---

## Airflow DAGs

| DAG ID | Platform | Task Sequence |
|---|---|---|
| `twitter_pipeline` | Twitter | ingestion → processing → analytics |
| `twitter_pipeline_test` | Twitter | ingestion → dedup-raw → processing → dedup-processed → analytics |
| `youtube_pipeline` | YouTube | ingestion → processing → sentiment → stance → conversation-threads → gemini-analysis |
| `youtube_gemini_analysis` | YouTube | gemini-analysis (standalone re-run on existing threads) |
| `facebook_pipeline` | Facebook | ingestion → processing → analytics → gemini-analysis → stance-analysis |

All DAGs are manual-trigger only (`schedule=None`, `catchup=False`). Twitter retries 3×; YouTube retries 2×; retry delay 5 minutes.

Processing tasks run on the Spark cluster (`spark://spark-master:7077`, 6 total executor cores, 4 GB executor memory). ML inference tasks run on local Spark (`local[*]`, single container) to keep model state in one process. All Spark jobs have a 2-hour execution timeout; the Twitter analytics task allows 3 hours.

---

## API Reference

Base URL: `http://localhost:8000`. All endpoints except `/api/auth/login` require `Authorization: Bearer <token>`.

### Auth

| Method | Endpoint | Body / Params | Description |
|---|---|---|---|
| `POST` | `/api/auth/login` | `{"username": "", "password": ""}` | Returns a UUID token. Tokens are in-memory; restart invalidates all sessions. |
| `POST` | `/api/auth/logout` | — | Discards the caller's token. |

### Pipeline

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/api/status` | Latest run state + task instances for all registered DAGs. |
| `GET` | `/api/runs` | Last 10 runs per DAG. |

### Superset

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/api/superset/config` | Platform → dashboard-ID mapping and Superset base URL. |
| `GET` | `/api/superset/guest-token?dashboard_id=<id>` | Issues a short-lived Superset guest token for embedding. |

### Insights

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/api/youtube/thread-insights` | Pre-computed YouTube conversation thread insights from S3. |
| `GET` | `/api/facebook/controversial-posts` | Pre-computed Facebook controversial post scores from S3. |

---

## Configuration System

Config is loaded once per process via `lru_cache`. `base.yaml` is always applied; `{ENV}.yaml` is merged on top.

| `ENV` | S3 Bucket |
|---|---|
| `dev` | `social-analysis-dev-bucket` |
| `test` | `social-analysis-test-bucket` |
| `prod` | `social-analysis-prod-bucket` |

Always set `ENV=test` when running tests to avoid touching dev or prod buckets.

---

## Docker Setup

### Build the pipeline image

The `Dockerfile.pipeline` extends `apache/spark:3.5.0` with a Python venv, HuggingFace Transformers, PyTorch, and bundled ML models. Models must be present locally before building:

- `src/ml/models/sentiment/`
- `src/ml/models/ytb_sentiment/`
- `src/ml/models/zeroshot/`

```bash
cd infra/docker
docker compose build spark-worker-1 spark-worker-2 spark-submit
```

### Service overview

| Service | Image | Purpose |
|---|---|---|
| `postgres` | postgres:16 | Airflow metadata DB |
| `airflow-init` | apache/airflow:3.2.0 | One-shot DB init + admin user creation |
| `airflow-apiserver` | apache/airflow:3.2.0 | REST API + UI (port 8080) |
| `airflow-scheduler` | apache/airflow:3.2.0 | DAG scheduling |
| `airflow-dag-processor` | apache/airflow:3.2.0 | DAG file parsing |
| `spark-master` | apache/spark:3.5.0 | Spark standalone master (port 7077) |
| `spark-worker-1/2` | Dockerfile.pipeline | Spark workers (3 cores, 6 GB each) |
| `spark-submit` | Dockerfile.pipeline | Airflow-triggered spark-submit entry point |
| `superset-db` | postgres:17 | Superset metadata DB |
| `superset-redis` | redis:7 | Celery broker + cache |
| `superset-init` | apache/superset:latest | One-shot Superset bootstrap |
| `superset` | apache/superset:latest | Superset app (port 8088, gunicorn) |
| `superset-worker` | apache/superset:latest | Celery async chart rendering |
| `superset-worker-beat` | apache/superset:latest | Celery scheduled tasks |

---

## Testing

### Run all tests

```bash
ENV=test PYTHONPATH=. pytest tests/ -v --tb=short
```

### Pure-Python tests (no Java/Spark required)

```bash
ENV=test PYTHONPATH=. pytest \
  tests/test_ingestion.py \
  tests/test_fb_conversation.py \
  tests/test_utils.py \
  -v --tb=short
```

### Spark tests (requires Java 17 + PySpark)

```bash
ENV=test PYTHONPATH=. PYSPARK_PYTHON=python3 pytest \
  tests/test_processing.py \
  tests/test_analytics.py \
  tests/test_fb_processing.py \
  tests/test_fb_analytics.py \
  tests/test_yt_processing.py \
  -v --tb=short
```

The `spark` fixture in `conftest.py` starts a `local[1]` SparkSession scoped to the entire test session. `pytest.importorskip("pyspark")` skips Spark tests silently when PySpark is not installed.

---

## CI/CD

### CI pipeline

```
on: push / PR → main

lint (ruff)
    │
    ├── test-pure   Python 3.11, no Java
    └── test-spark  Python 3.11, Java 17 (Temurin), pyspark==3.5.0
```

Both test jobs run in parallel after lint passes. Ruff excludes `src/ml/*.ipynb` and `tests/test_ai/`.

### CD pipeline

Not implemented. `cd.yml` contains a placeholder step and runs on push to `main`.

---

## Code Style

```bash
# Lint
ruff check src/ tests/

# Auto-fix
ruff check src/ tests/ --fix
```

CI blocks merges on any ruff violation. Run the check locally before pushing.

---

## Known Limitations

- **Facebook ingestion** relies on browser automation, which is sensitive to UI changes and rate limiting. It is not a stable production integration.
- **Dashboard auth** uses an in-memory token store. All active sessions are lost on server restart.
- **Gemini sentiment** batches the entire DataFrame into a single prompt; very large DataFrames will exceed the model's context window.
- **ML models are not in the repository.** They must be present at `src/ml/models/` before building the Docker image. Model paths are hard-coded in the inference modules.
- **CD pipeline** is not implemented. Deployment is currently manual.
- **Redshift loader** (`src/storage/redshift/loader.py`) is not wired into any DAG by default.

---

## Contributing

1. Branch off `main`.
2. Run `ruff check src/ tests/` before opening a PR — zero violations required.
3. Add or update tests for changed logic. Spark logic → Spark tests; pure-Python logic → pure-Python tests.
4. Use `ENV=test` when running tests locally.
5. Never commit `.env` or files containing credentials.
