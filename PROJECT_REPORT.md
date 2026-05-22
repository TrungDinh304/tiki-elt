# Tiki Lakehouse — Project Report

## Overview

This project is a small lakehouse pipeline for crawling Tiki e-commerce data, storing raw datasets in an object store (MinIO), transforming with `dbt` (DuckDB + parquet external materializations), exposing query access via Trino, and visualizing via Superset and a local analytics script.

Key entry points:
- DAGs: [dags/tiki_lakehouse_dag.py](dags/tiki_lakehouse_dag.py), [dags/tiki_category_dag.py](dags/tiki_category_dag.py)
- Crawlers: [crawler/fetch_tiki.py](crawler/fetch_tiki.py), [crawler/fetch_category.py](crawler/fetch_category.py)
- Transforms: `dbt` project at [dbt_tiki/](dbt_tiki)
- Infra: `docker-compose.yml`, [airflow/Dockerfile](airflow/Dockerfile)

## High-level Architecture & Data Flow

1. Data collection (bronze):
   - `crawler/fetch_tiki.py` (daily) and `crawler/fetch_category.py` (monthly) call Tiki APIs to fetch listings, product details, seller info, reviews, and menu-config.
   - Responses are normalized to pandas DataFrames and saved as parquet to MinIO using Hive-style partitioning: `s3://bronze/<entity>/dt=YYYY-MM-DD/run_id=.../*.parquet`.
   - Each crawl writes a success manifest to `_manifests/` for downstream detection.

2. Orchestration (Airflow):
   - DAG `tiki_lakehouse_daily_pipeline` runs: crawl → dbt run (staging+marts) → archive processed bronze → analytics_plot.
   - DAG `tiki_category_monthly_pipeline` runs category crawl + category-only dbt refresh.

3. Transform (dbt + DuckDB):
   - `dbt-duckdb` runs SQL models configured as `external` parquet materializations (staging + marts) per `dbt_project.yml`.
   - DuckDB reads/writes parquet and the local DuckDB file (`dbt_tiki/tiki.duckdb`) is the dbt target.

4. Query & Visualization:
   - Trino is configured to query parquet files and uses Postgres as metadata backend where needed.
   - Superset connects to Trino/Postgres for dashboards.
   - `scripts/analytics_plot.py` produces static plots as a final DAG step.

5. Archive & Idempotency:
   - `crawler/archive_processed.py` moves consumed partitions to `_processed/` to avoid repeated processing by dbt.

## Components & Responsibilities (detailed)
  - Docker Compose (`docker-compose.yml`)
    - Responsibility: provision local development infrastructure and service topology (MinIO, Postgres, Trino, Superset, Redis, Airflow).
    - Key configs: port mappings, network `lakehouse_net`, volumes for persistence, environment variables passed into containers (minio/postgres creds, bucket names, DAG/vault paths).
    - Runtime: `docker compose up -d` or `make up`.
    - Outputs / artifacts: running containers, initialized MinIO buckets via `minio-init`.

  - Apache Airflow
    - Responsibility: schedule and orchestrate pipeline tasks (crawls, dbt runs, archiving, analytics). Provides retries, logging, and DAG-level dependencies.
    - How it runs: Airflow container built from `airflow/Dockerfile` creates `/opt/project-venv` with crawler/dbt deps. DAGs call venv binaries explicitly (e.g. `/opt/project-venv/bin/python crawler/fetch_tiki.py`).
    - Key files: [dags/tiki_lakehouse_dag.py](dags/tiki_lakehouse_dag.py), [dags/tiki_category_dag.py](dags/tiki_category_dag.py), `airflow/start.sh` (entrypoint bootstrap).
    - Observability: Airflow UI (webserver) shows DAG runs, task logs; task stdout/stderr captured to mounted `airflow_home/logs`.
    - Failure modes: task command errors, missing binaries in venv, misconfigured env vars; Airflow provides retries and visibility.

  - Crawlers (`crawler/`)
    - `fetch_tiki.py`
      - Responsibility: crawl product listing pages for chosen categories, fetch per-product details, seller info, and reviews; write partitioned parquet files to MinIO bronze bucket; produce success manifest and invoke watermark updates.
      - Inputs: category list (from `category_selector.plan_crawl`), environment variables (`MINIO_ENDPOINT`, `MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY`, `BRONZE_BUCKET`, `TRIGGER_DBT_EVERY_N`, `DBT_BIN`, `DBT_PROJECT_DIR`, paging and delay settings).
      - Outputs: parquet files under `bronze/tiki_products`, `bronze/tiki_product_details`, `bronze/tiki_sellers`, `bronze/tiki_reviews`; `_manifests/dt=.../run_id.json` marker.
      - Key functions: `fetch_products_for_category()`, `process_page()`, `save_to_minio()`, `_run_dbt_staging_refresh()`, `write_success_marker()`.
      - Runtime: invoked by Airflow `BashOperator` or `make crawl`; handles retries and backoff for HTTP requests.
      - Failure modes & handling: resilient to network/API failures (retry + delay), optional Redis caching; interleaved dbt failures are logged but non-fatal.

    - `fetch_category.py`
      - Responsibility: fetch Tiki menu-config, flatten nested tree to rows, write categories to bronze and emit manifest.
      - Inputs: `CATEGORY_URL`, MinIO env variables.
      - Outputs: `bronze/tiki_categories/dt=.../categories.parquet`, `_manifests/tiki_categories/...`.
      - Key functions: `fetch_menu()`, `flatten_categories()`, `save_to_minio()`.

  - MinIO
    - Responsibility: provide S3-compatible object storage for raw (bronze), staged (silver), and final (lakehouse) parquet artifacts.
    - How it's used: crawler uses boto3 `put_object`; dbt/duckdb write/read parquet via S3 endpoints; Trino configured to read parquet files from MinIO.
    - Operational considerations: buckets created by `minio-init`; endpoint differs between host vs container (`http://minio:9000` inside compose).

  - dbt + DuckDB
    - Responsibility: transform bronze raw data into curated staging and marts, materializing parquet files that serve analytic consumers.
    - How it runs: `dbt` CLI installed in project venv; DAG runs `cd dbt_tiki && /opt/project-venv/bin/dbt run --profiles-dir .` with `--vars` to provide bucket names.
    - Config: `dbt_project.yml` sets `staging` and `marts` as `external` parquet models; individual models call `{{ config(location=external_path('silver')) }}`.
    - Outputs: parquet files in `silver`/`lakehouse` bucket paths and `target/` run artifacts (manifests, run_results).

  - Trino
    - Responsibility: provide a fast distributed SQL layer over parquet files for BI (Superset) and ad-hoc queries.
    - Integration points: reads from MinIO (MinIO S3 connector) and uses Postgres metastore where configured.
    - Runtime: configured via `trino/etc` mounted into container; started in compose as `trino` service.

  - Superset
    - Responsibility: provide dashboards and exploration UI; visualizes Trino query results and can cache queries.
    - How it runs: container entrypoint installs `trino` client, bootstraps DB and admin user, runs `superset init` and Gunicorn.
    - Security: admin user created from env vars in compose; SQLAlchemy connection configured to Postgres.

  - Postgres
    - Responsibility: metadata store for Airflow (DAG/task state), Superset (app DB), and Trino metastore if used.
    - How it's used: single Postgres container in compose; persistent volume `pg_data` for durability.

  - Redis (`redis/redis.py`)
    - Responsibility: lightweight API response caching to reduce repeated calls to slow endpoints (seller info); optional accelerator for Superset cache.
    - Behavior: helper forms cache keys by URL+params, uses `Redis.get/setex`, TTL default 86400s, gracefully degrades if Redis client missing.

  - Archive utility (`crawler/archive_processed.py`)
    - Responsibility: perform post-dbt housekeeping — atomically move processed partition objects to `_processed/` to mark them as consumed and keep bronze listing clean.
    - Implementation: uses boto3 `list_objects_v2` paginator, `copy_object` then `delete_object` per key; supports `--dt` and `--entities` filters.

  - Analytics plotting (`scripts/analytics_plot.py`)
    - Responsibility: produce reproducible static analytics plots (matplotlib/seaborn) from transformed parquet/duckdb outputs; final DAG task for report generation.
    - Invocation: `BashOperator` runs `python scripts/analytics_plot.py` in project venv; outputs saved locally to `images/`.

  - Dev tooling (Makefile, pyproject.toml, uv)
    - Responsibility: developer convenience tasks: venv creation (`uv venv`), dependency install, pre-commit setup, local run shortcuts (`make crawl`, `make dbt-run`, `make up`).
    - How it's used: simplifies reproducing the environment and running components locally.

## Integration & Operational Details

- Environment variables and credentials flow from `.env` into `docker-compose.yml` and into container envs. Inside Airflow tasks, `MINIO_ENDPOINT` is set to `http://minio:9000` so boto3 and dbt point at the MinIO container.
- `fetch_tiki.py` interleaves short `dbt run --select staging` invocations to refresh silver mid-crawl using a resolved `DBT_BIN` executable; failures are logged but non-fatal.
- `archive_processed.py` performs object copy+delete (MinIO copy_object + delete_object) to preserve history under `_processed/`.
- `redis/redis.py` gracefully degrades if Redis client not present; caching TTL is configurable via `CACHE_TTL`.

## Runbook / Quick Setup (local dev)

1. Create `.env` with required secrets (MinIO root user/password, Postgres credentials, etc.).
2. Start infrastructure:

```bash
make up
# or
docker compose up -d
```

3. Run a category crawl (one-off):

```bash
make crawl-categories
```

4. Run a full crawl locally:

```bash
make crawl
```

5. Run dbt transforms:

```bash
make dbt-run
# or inside project venv
cd dbt_tiki && uv run dbt run --vars "{bronze_bucket: bronze, silver_bucket: silver, lakehouse_bucket: lakehouse}"
```

6. Archive processed partitions:

```bash
make archive
# or
uv run python crawler/archive_processed.py --dt 2026-05-21
```

## Files of Interest

- [dags/tiki_lakehouse_dag.py](dags/tiki_lakehouse_dag.py)
- [dags/tiki_category_dag.py](dags/tiki_category_dag.py)
- [crawler/fetch_tiki.py](crawler/fetch_tiki.py)
- [crawler/fetch_category.py](crawler/fetch_category.py)
- [crawler/archive_processed.py](crawler/archive_processed.py)
- [dbt_tiki/dbt_project.yml](dbt_tiki/dbt_project.yml)
- [docker-compose.yml](docker-compose.yml)
- [airflow/Dockerfile](airflow/Dockerfile)
- [redis/redis.py](redis/redis.py)
- [Makefile](Makefile)

## Recommendations & Next Steps

- Add a README `docs/` page with environment variable examples and `.env.example`.
- Add CI checks to run dbt tests and crawler unit tests (`crawler/tests/test_fetch.py`).
- Consider adding a small health-check DAG or alerting on failed DAG runs.
- Optional: generate a Mermaid architecture diagram for documentation.

---

Report generated from repository scan on 2026-05-21.
