# Tiki Lakehouse

A self-contained lakehouse demo for Tiki product and review analytics. This project combines Python ingestion, DuckDB/dbt transformations, Trino query serving, and Superset visualization on top of MinIO object storage.

---

## What this project does

- Crawls Tiki product, review, category, and seller data.
- Stores raw data in MinIO as Parquet files.
- Transforms raw files with `dbt` and `DuckDB` into staging and marts.
- Uses Trino and Apache Iceberg metadata for SQL serving.
- Provides BI dashboards through Superset.
- Orchestrates pipeline tasks with Airflow.

---

## Architecture

| Layer | Tool | Purpose |
| :--- | :--- | :--- |
| Ingestion | Python crawler | Fetches Tiki API data and writes Parquet to MinIO |
| Storage | MinIO | S3-compatible object store for Bronze/Silver/Gold data |
| Transformation | dbt + DuckDB | Reads raw Parquet, cleans data, writes modeled outputs |
| Metadata | PostgreSQL | Stores Iceberg catalog metadata and Superset DB |
| Query Engine | Trino | Serves Iceberg tables over MinIO |
| Visualization | Superset | Dashboards and analytics UI |
| Orchestration | Airflow | Runs the pipeline end-to-end |

---

## Project structure

```text
.tiki-lakehouse/
├── .github/workflows/       # CI checks for formatting and SQL linting
├── airflow/                 # Airflow Docker build context
├── airflow_home/            # Airflow runtime files and logs
├── crawler/                 # Python ingestion scripts and helpers
│   ├── tests/               # Pytest unit tests
│   └── fetch_tiki.py        # Main crawler entrypoint
├── dags/                    # Airflow DAG definitions
│   ├── tiki_lakehouse_dag.py
│   └── tiki_dbt_refresh_dag.py
├── dbt_tiki/                # dbt project and DuckDB profile
│   ├── models/              # staging and marts SQL models
│   ├── dbt_project.yml
│   └── profiles.yml
├── scripts/                 # Scripts invoked by DAGs / bootstrap
│   ├── analytics_plot.py    # Generates chart PNGs from marts (DAG task)
│   └── run_project.sh       # One-shot project bootstrap
├── tools/                   # Manual debug / inspection utilities
│   ├── check_buckets.py     # List MinIO bucket contents
│   └── check_duckdb.py      # Inspect local tiki.duckdb tables
├── trino/                   # Trino configuration and catalogs
│   └── etc/catalog/
├── Makefile                 # Common local commands
├── docker-compose.yml       # Infrastructure services stack
└── pyproject.toml           # Python package and dependency config
```

---

## Prerequisites

- Docker and Docker Compose
- Python 3.10+
- `uv` package manager
- A `.env` file with required credentials

---

## Setup

Create or update your `.env` file with credentials for:

- `MINIO_ROOT_USER`
- `MINIO_ROOT_PASSWORD`
- `POSTGRES_USER`
- `POSTGRES_PASSWORD`
- `POSTGRES_DB`
- `SUPERSET_SECRET_KEY`
- `SUPERSET_ADMIN_USER`
- `SUPERSET_ADMIN_PASSWORD`
- `SUPERSET_ADMIN_EMAIL`
- `AIRFLOW_ADMIN_USER`
- `AIRFLOW_ADMIN_PASSWORD`
- `AIRFLOW_ADMIN_EMAIL`
- `MINIO_ACCESS_KEY`
- `MINIO_SECRET_KEY`
- `BRONZE_BUCKET`
- `SILVER_BUCKET`
- `LAKEHOUSE_BUCKET`
- `BUDGET_PER_RUN`
- `PAGES_PER_CATEGORY`

Then install dependencies:

```bash
make setup
```

---

## Start local stack

```bash
make up
```

`make up` brings services up in dependency order (infra → BI/orchestrator →
chatbot), waits for Airflow's metadata DB to be reachable, and prints all
service URLs. Works the same in PowerShell, Git Bash, or a POSIX shell because
the recipe runs through GNU Make's shell — you don't need to copy multi-line
shell loops into your terminal.

Services brought up:

- MinIO + `minio-init` (creates `bronze` / `silver` / `lakehouse` buckets)
- PostgreSQL (pgvector)
- Trino
- Superset
- Redis
- Airflow
- chatbot (Streamlit RAG UI — calls an external OpenAI-compatible LLM gateway, see RAG section)

Raw equivalent (if you don't want to use Make): `docker compose --env-file .env up -d`.

---

## First-time bootstrap / Full reset

Use this when starting from an empty state (no volumes) or after `docker compose down -v`.

### Caveats before wiping volumes

- The `hf_cache` volume stores the **BGE-M3 model (~2.3GB)**. Wiping it forces a
  re-download (~10–30 min) on the next `rag-indexer-init` or `chatbot` start.
  Prefer **Option B (selective wipe)** below to preserve it.
- `.env` is bind-mounted, so the LLM gateway credentials (`LLM_BASE_URL`,
  `LLM_API_KEY`, `CHAT_MODEL`) survive `down -v`. The chatbot reaches a
  gateway running on the docker host via `host.docker.internal:<port>` —
  make sure the gateway (e.g. 9router) is actually running before opening
  the chatbot, otherwise streaming requests fail with `Connection refused`.
- After `airflow_db` is wiped, Airflow recreates the admin user with a new
  internal id. Any stale browser cookie at `http://localhost:8081` returns
  HTTP 500 — **clear cookies / hard refresh** after the airflow container reboots.
- `BUDGET_PER_RUN=1` in `.env` only crawls **one leaf category per run** — the
  RAG index will be very narrow. Bump to `BUDGET_PER_RUN=20` (or higher) before
  the first crawl if you want broader coverage out of the box. With
  `PAGES_PER_CATEGORY=20` and 1.5–3.5 s request delays, ~20 categories take 30–60 min.

### Option A — wipe everything (slowest first run)

```bash
docker compose down -v
```

### Option B — wipe state, keep model cache (recommended)

List volumes first (`docker volume ls | findstr tiki-elt` in PowerShell, or
`grep` in bash) to confirm exact names, then remove the state volumes —
keep `hf_cache` to avoid re-downloading BGE-M3:

```bash
docker compose down
docker volume rm tiki-elt_minio_data tiki-elt_pg_data tiki-elt_trino_metastore tiki-elt_airflow_db
```

### Bring the stack up + bootstrap

```bash
make up              # docker bring-up in dependency order, waits for Airflow ready
make bootstrap       # unpause all DAGs + trigger tiki_category_monthly_pipeline
```

Both targets are cross-shell (PowerShell / Git Bash / POSIX) — Make runs the
recipe through its own shell, so the bash-style loop inside `bootstrap` works
even when you invoke it from PowerShell.

`make bootstrap` triggers the category DAG only. Wait for it to finish in the
Airflow UI ([http://localhost:8081](http://localhost:8081), ~1–2 min), then
kick off the main pipeline:

```bash
docker compose exec airflow airflow dags trigger tiki_lakehouse_daily_pipeline
```

The main pipeline runs `crawl → dbt → archive → analytics + rag_index` and
populates pgvector at the end. The chatbot at
[http://localhost:8501](http://localhost:8501) returns empty results until this
first run finishes.

### Verify each stage

```bash
# Marts materialized?
docker compose exec airflow ls /opt/project/dbt_tiki/target/

# Embeddings landed in pgvector? (single line — works in PowerShell + bash)
docker compose exec postgres psql -U admin -d metastore -c "SELECT category_name, COUNT(*) FROM rag.product_embeddings GROUP BY 1 ORDER BY 2 DESC LIMIT 10;"

# Bronze partitions in MinIO — open http://localhost:9001 → bronze bucket
```

### Filling specific category gaps later

Once the stack is running, you can add coverage for a missing category without
re-running the full daily pipeline:

```bash
make crawl-cats IDS=8322,1882                   # comma-separated leaf ids
docker compose exec airflow airflow dags trigger tiki_rag_indexer
```

---

## Run the data pipeline

Run ingestion:

```bash
make crawl
```

Run dbt transformations:

```bash
make dbt-run
```

Archive processed source partitions:

```bash
make archive
```

Launch Airflow standalone:

```bash
make airflow-start
```

---

## Useful service URLs

| Service | URL |
| :--- | :--- |
| MinIO Console | `http://localhost:9001` |
| Trino UI | `http://localhost:8080` |
| Superset | `http://localhost:8088` |
| Airflow | `http://localhost:8081` |
| Chatbot (RAG) | `http://localhost:8501` |

---

## Developer commands

| Command | Description |
| :--- | :--- |
| `make setup` | Create local environment and install dependencies |
| `make up` | Bring up the full stack in dependency order (waits for Airflow) |
| `make bootstrap` | One-shot: unpause DAGs + trigger category bootstrap (after a fresh `airflow_db`) |
| `make down` | Stop Docker services |
| `make crawl` | Run the Tiki crawler (watermark-rotated, picks oldest categories) |
| `make crawl-cats IDS=...` | Crawl specific leaf category ids (bypasses rotation) |
| `make crawl-categories` | Run the category crawler |
| `make dbt-run` | Run dbt models with DuckDB |
| `make archive` | Archive processed bronze partitions |
| `make lint` | Run formatting and linting checks |
| `make test` | Run crawler unit tests |
| `make airflow-start` | Run Airflow standalone |

---

## Testing and linting

Run Python formatting and linting:

```bash
uv run black crawler/
uv run flake8 crawler/
```

Run SQL linting for dbt models:

```bash
cd dbt_tiki && uv run sqlfluff lint models --dialect duckdb
```

Run unit tests:

```bash
uv run pytest crawler/tests/
```

---

## Pipeline summary

1. Bronze: Python crawler writes raw Tiki data into MinIO as Parquet.
2. Silver: dbt staging models read raw Parquet with DuckDB and normalize fields.
3. Gold: dbt mart models produce analytical tables for consumption.
4. Serving: Trino reads Iceberg metadata and queries data from MinIO.
5. Visualize: Superset connects to Trino for dashboards.

---

## RAG chatbot

A Streamlit chatbot tư vấn sản phẩm Tiki sits on top of the lakehouse marts:

1. `tiki_rag_indexer` DAG (or `python scripts/rag_index.py` manually) reads
   `dim_products` + top-5 reviews per product, embeds each product **locally**
   with sentence-transformers (BGE-M3, multilingual), and upserts into
   `rag.product_embeddings` on Postgres (pgvector). No external API call.
2. The `chatbot` service (port 8501) does hybrid retrieval (structured price
   / rating / category filters → pgvector cosine top-K) and streams the answer
   from an external OpenAI-compatible LLM gateway (defaults to 9router
   running on the docker host).

The BGE-M3 weights (~2.3GB) download on the first indexer or chatbot run into
the shared `hf_cache` docker volume, so subsequent containers reuse them.

Setup:

```bash
# 1. Start your LLM gateway on the host (9router, OpenRouter local proxy,
#    Ollama with OpenAI mode, etc). Default config points at
#    http://host.docker.internal:20128/v1 — adjust LLM_BASE_URL in .env if
#    yours listens on a different port.

# 2. Set LLM_API_KEY + CHAT_MODEL in `.env` to whatever your gateway expects.
#    (See .env.example for all RAG-related vars.)

# 3. Bring everything up. `rag-indexer-init` runs once and populates
#    rag.product_embeddings from existing marts.
docker compose up -d --build

# 4. Open the chat UI: http://localhost:8501
```

**Network note**: the chatbot container reaches the host gateway via
`host.docker.internal:<port>`. Compose sets `extra_hosts:
host.docker.internal:host-gateway` so this works on Linux too (Docker Desktop
provides it automatically on Mac/Windows). If you see `Connection refused`,
the gateway either isn't running or is bound to `127.0.0.1` only — bind it
to `0.0.0.0:<port>` so containers can reach it.

Re-indexing happens automatically at 07:00 daily via the `tiki_rag_indexer`
DAG. To force a re-index manually:

```bash
docker exec tiki_airflow /opt/project-venv/bin/python /opt/project/scripts/rag_index.py
```

## Notes

- `dbt_tiki/profiles.yml` is configured for DuckDB with MinIO S3 access.
- The Airflow container maps local DAGs and crawler code for easy development.
- `docker compose` uses `.env` values to configure services and credentials.
- Postgres image is `pgvector/pgvector:pg15` (drop-in for `postgres:15`) so the
  RAG indexer can `CREATE EXTENSION vector` on the existing metastore DB.

---

## Goal

This repository is a hands-on lakehouse demo for e-commerce analytics. It is built to be easy to run locally, inspect with SQL, and extend with new models or sources.



## Explain data flow

```
Daily DAG:
  crawl_tiki_data  ────────────────────────────────────────►  run_dbt_marts ─► archive ─► analytics
    │                                                               │
    │  TRIGGER_DBT_EVERY_N=5 (set qua task env)                     │
    │                                                               │
    ├─ cat 1..5 crawl → dbt run --select path:models/staging  ◄── Trino thấy data NGAY
    ├─ cat 6..10 crawl → dbt staging                          ◄── Silver fresh hơn
    ├─ cat 11..15 ...                                                 
    └─ cat …50 done                                                 
                                                                    ▼
                                                     dbt run --select path:models/marts
                                                       (chỉ rebuild dim_*/fct_* — stg đã fresh)

```
