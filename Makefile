.PHONY: setup up down crawl dbt-run archive lint test

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

# Run dbt transformations
dbt-run:
	cd dbt_tiki && uv run dbt run --vars "{bronze_bucket: bronze, silver_bucket: silver, lakehouse_bucket: lakehouse}"

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
	export AIRFLOW_HOME=$(PWD)/airflow_home && \
	export AIRFLOW__WEBSERVER__WEB_SERVER_PORT=8081 && \
	uv run airflow standalone
