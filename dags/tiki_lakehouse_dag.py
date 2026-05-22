from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta
from pathlib import Path

# Resolve project root from this file's location so the DAG is portable
# across machines (no hard-coded absolute paths).
PROJECT_ROOT = str(Path(__file__).resolve().parent.parent)

# Crawler + dbt live in a dedicated venv inside the Airflow image (see
# airflow/Dockerfile) to avoid clashing with Airflow's own pinned deps.
PROJECT_PY = "/opt/project-venv/bin/python"
PROJECT_DBT = "/opt/project-venv/bin/dbt"

default_args = {
    'owner': 'tiki_admin',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'tiki_lakehouse_daily_pipeline',
    default_args=default_args,
    description='Pipeline chạy hàng ngày để cào dữ liệu, transform và vẽ báo cáo',
    schedule='0 0 * * *', # Chạy vào lúc 0h sáng hàng ngày
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['tiki', 'lakehouse'],
) as dag:

    # Task 1: Crawl. Every 5 categories the crawler fires a dbt staging refresh
    # so silver tables become queryable mid-crawl via Trino/Superset instead of
    # waiting until the crawl finishes. `execution_timeout` caps the task so
    # Airflow forcibly kills it after 45 minutes instead of hanging — partial
    # crawls still leave plenty of fresh bronze for the downstream dbt task.
    task_crawl = BashOperator(
        task_id='crawl_tiki_data',
        bash_command=f"cd {PROJECT_ROOT} && {PROJECT_PY} crawler/fetch_tiki.py",
        env={
            "TRIGGER_DBT_EVERY_N": "5",
            "DBT_BIN": PROJECT_DBT,
            "DBT_PROJECT_DIR": f"{PROJECT_ROOT}/dbt_tiki",
        },
        append_env=True,
        execution_timeout=timedelta(minutes=45),
    )

    # Task 2: rebuild staging + marts. `trigger_rule=ALL_DONE` means this runs
    # regardless of whether crawl succeeded, failed, or hit its timeout — so a
    # stuck crawl never blocks marts from refreshing on whatever bronze the
    # crawler did manage to write (plus the interleaved staging refreshes that
    # already populated silver mid-crawl).
    task_dbt = BashOperator(
        task_id='run_dbt',
        bash_command=(
            f"cd {PROJECT_ROOT}/dbt_tiki && "
            f"{PROJECT_DBT} run --profiles-dir . "
            '--vars "{bronze_bucket: bronze, silver_bucket: silver, lakehouse_bucket: lakehouse}"'
        ),
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # Task 3: Archive bronze data đã được dbt xử lý → _processed/
    # Chỉ chạy SAU khi dbt run thành công để tránh mất data nếu transform fail.
    task_archive = BashOperator(
        task_id='archive_processed_bronze',
        bash_command=f"cd {PROJECT_ROOT} && {PROJECT_PY} crawler/archive_processed.py",
    )

    # Task 4: Chạy script analytics để vẽ biểu đồ mới
    task_analytics = BashOperator(
        task_id='generate_analytics_report',
        bash_command=f"cd {PROJECT_ROOT} && {PROJECT_PY} scripts/analytics_plot.py",
    )

    # Luồng: Crawl → dbt (staging + marts) → Archive → Analytics
    task_crawl >> task_dbt >> task_archive >> task_analytics
