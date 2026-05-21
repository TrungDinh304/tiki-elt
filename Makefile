.PHONY: setup up down crawl crawl-categories dbt-run archive lint test airflow-start
# read env
include .env
# Setup global environment and dependencies
setup:
	uv venv
	uv pip install -e .[dev]
	uv pip install pre-commit --system || true
	pre-commit install

# Start Local Infrastructure
up:
	docker compose up -d 

# Stop Local Infrastructure
down:
	docker compose down

# Run the crawler
crawl:
	uv run python crawler/fetch_tiki.py

# Run the category crawler (monthly cadence)
crawl-categories:
	uv run python crawler/fetch_category.py

# Run dbt transformations
dbt-run:
	cd dbt_tiki && uv run dbt run --vars "{bronze_bucket: ${BRONZE_BUCKET}, silver_bucket: ${SILVER_BUCKET}, lakehouse_bucket: ${LAKEHOUSE_BUCKET}}"

# Archive processed bronze partitions to _processed/
archive:
	uv run python crawler/archive_processed.py

# Run linters
lint:
	uv run black crawler/
	uv run flake8 crawler/
	cd dbt_tiki && uv run sqlfluff lint models

# Run tests
test:
	uv run pytest crawler/tests/

# Start Airflow Standalone
airflow-start:
	airflow standalone
