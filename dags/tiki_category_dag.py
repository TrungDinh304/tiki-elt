"""Monthly category crawl + dbt refresh of dim_categories.

Categories on Tiki change infrequently, so this runs on the 1st of every
month and is kept separate from the daily product/seller/review pipeline in
tiki_lakehouse_dag.py.
"""
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from pathlib import Path

# Resolved from this file's location so the DAG works on any host.
PROJECT_ROOT = str(Path(__file__).resolve().parent.parent)

# Crawler + dbt live in a dedicated venv inside the Airflow image.
PROJECT_PY = "/opt/project-venv/bin/python"
PROJECT_DBT = "/opt/project-venv/bin/dbt"

default_args = {
    "owner": "tiki_admin",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "tiki_category_monthly_pipeline",
    default_args=default_args,
    description="Crawl Tiki menu-config monthly and refresh dim_categories",
    schedule="0 0 1 * *",  # 00:00 on day 1 of every month
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["tiki", "lakehouse", "category"],
) as dag:

    task_crawl_categories = BashOperator(
        task_id="crawl_tiki_categories",
        bash_command=f"cd {PROJECT_ROOT} && {PROJECT_PY} crawler/fetch_category.py",
    )

    # Only rebuild the category-related models so we don't accidentally
    # refresh daily marts on a monthly cadence. DBT_TARGET_PATH ra ngoài
    # bind-mount để tránh xung đột UID trên manifest.json (xem
    # tiki_lakehouse_dag.py để biết chi tiết).
    task_dbt_categories = BashOperator(
        task_id="run_dbt_categories",
        env={"DBT_TARGET_PATH": "/tmp/dbt_target"},
        append_env=True,
        bash_command=(
            f"cd {PROJECT_ROOT}/dbt_tiki && "
            f"{PROJECT_DBT} run --profiles-dir . --select stg_tiki_categories+ "
            '--vars "{bronze_bucket: bronze, silver_bucket: silver, lakehouse_bucket: lakehouse}"'
        ),
    )

    task_archive_categories = BashOperator(
        task_id="archive_processed_categories",
        bash_command=(
            f"cd {PROJECT_ROOT} && "
            f"{PROJECT_PY} crawler/archive_processed.py --entities tiki_categories"
        ),
    )

    task_crawl_categories >> task_dbt_categories >> task_archive_categories
