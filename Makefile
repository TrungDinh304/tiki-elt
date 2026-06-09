.PHONY: setup up bootstrap down crawl crawl-cats crawl-categories dbt-run archive lint test airflow-start

# Recipes phải cmd.exe-compatible — GNU Make trên Windows ignore `SHELL := bash`
# trong nhiều phiên bản và rơi xuống cmd. Quy tắc: KHÔNG dùng bash control flow
# (`until`/`for d in`/`[ -z $X ]`) trong recipe. Cho việc cần loop hoặc poll,
# tách ra Python helper trong `scripts/` (cross-platform vì Python luôn có trong
# venv). Cho conditional, dùng Make-level `$(if)` / `ifeq`.

# read env
include .env
# Setup global environment and dependencies
setup:
	uv venv
	uv pip install -e .[dev]
	uv pip install pre-commit --system || true
	pre-commit install

# Bring up the full stack in dependency order (match README "First-time
# bootstrap" section). Idempotent — chạy lại an toàn, compose chỉ recreate
# service nào thay đổi. Stage 1: infra + bucket init. Stage 2: orchestrator.
# Stage 3: chờ Airflow CLI gọi được (= metadata DB sẵn sàng). Stage 4: chatbot
# (UI lên ngay; retrieval rỗng cho tới khi pipeline chạy lần đầu). Sau lần
# đầu, chạy `make bootstrap`; lần sau chỉ cần `make up`.
up:
	docker compose up -d minio postgres redis trino
	docker compose up minio-init
	docker compose up -d airflow
	python scripts/wait_for_airflow.py
	docker compose up -d chatbot
	@echo Stack up.
	@echo   Airflow:  http://localhost:8081
	@echo   Chatbot:  http://localhost:8501
	@echo   Trino:    http://localhost:8080
	@echo   MinIO:    http://localhost:9001
	@echo First-time setup? Run 'make bootstrap' to unpause + trigger DAGs.

# Unpause từng DAG + trigger category bootstrap. Chạy sau `make up` trên fresh
# airflow_db. Category DAG chạy trước để bronze có leaf ids; main DAG (trigger
# thủ công sau khi category DAG xong) sẽ pick từ đó.
bootstrap:
	docker compose exec -T airflow airflow dags unpause tiki_category_monthly_pipeline
	docker compose exec -T airflow airflow dags unpause tiki_lakehouse_daily_pipeline
	docker compose exec -T airflow airflow dags unpause tiki_dbt_refresh
	docker compose exec -T airflow airflow dags unpause tiki_rag_indexer
	docker compose exec -T airflow airflow dags trigger tiki_category_monthly_pipeline
	@echo Categories DAG triggered. Wait ~1-2 min in UI, then:
	@echo   docker compose exec airflow airflow dags trigger tiki_lakehouse_daily_pipeline

# Stop Local Infrastructure
down:
	docker compose down

# Run the crawler
crawl:
	uv run python crawler/fetch_tiki.py

# Crawl theo danh mục mong muốn (bỏ qua watermark rotation).
# IDS là chuỗi category leaf id, phân tách bằng dấu phẩy. Ví dụ:
#   make crawl-cats IDS=8322,316
# Chạy bên trong container airflow để DNS `minio:9000` resolve được + dùng
# venv project (đã có boto3/duckdb). Sau khi xong, refresh marts + embeddings
# để chatbot dùng dữ liệu mới:
#   make dbt-run && docker compose run --rm rag-indexer-init
crawl-cats:
ifeq ($(strip $(IDS)),)
	$(error Usage: make crawl-cats IDS=8322,316)
endif
	docker compose exec -T -e CRAWL_CATEGORY_IDS="$(IDS)" airflow sh -c "cd /opt/project && /opt/project-venv/bin/python crawler/fetch_tiki.py"

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
