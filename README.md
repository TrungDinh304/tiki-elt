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
│   └── tiki_lakehouse_dag.py
├── dbt_tiki/                # dbt project and DuckDB profile
│   ├── models/              # staging and marts SQL models
│   ├── dbt_project.yml
│   └── profiles.yml
├── trino/                   # Trino configuration and catalogs
│   └── etc/catalog/
├── Makefile                 # Common local commands
├── docker-compose.yml       # Infrastructure services stack
├── run_project.sh           # Convenience startup script
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
docker compose --env-file .env up -d
```

This brings up:

- MinIO
- PostgreSQL
- Trino
- Superset
- Redis
- Airflow

The compose stack also initializes the required MinIO buckets.

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

---

## Developer commands

| Command | Description |
| :--- | :--- |
| `make setup` | Create local environment and install dependencies |
| `make up` | Start Docker services |
| `make down` | Stop Docker services |
| `make crawl` | Run the Tiki crawler |
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

## Notes

- `dbt_tiki/profiles.yml` is configured for DuckDB with MinIO S3 access.
- The Airflow container maps local DAGs and crawler code for easy development.
- `docker compose` uses `.env` values to configure services and credentials.

---

## Goal

This repository is a hands-on lakehouse demo for e-commerce analytics. It is built to be easy to run locally, inspect with SQL, and extend with new models or sources.
