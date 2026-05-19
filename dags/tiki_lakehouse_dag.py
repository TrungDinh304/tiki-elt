from airflow import DAG
from airflow.operators.bash import BashOperator
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

    # Task 1: Chạy crawler — dùng python trong project venv (xem Dockerfile).
    task_crawl = BashOperator(
        task_id='crawl_tiki_data',
        bash_command=f"cd {PROJECT_ROOT} && {PROJECT_PY} crawler/fetch_tiki.py",
    )

    # Task 2: dbt transform — chạy dbt từ project venv.
    task_dbt = BashOperator(
        task_id='run_dbt_transformation',
        bash_command=(
            f"cd {PROJECT_ROOT}/dbt_tiki && "
            f"{PROJECT_DBT} run --profiles-dir . "
            '--vars "{bronze_bucket: bronze, silver_bucket: silver, lakehouse_bucket: lakehouse}"'
        ),
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
        bash_command=f"cd {PROJECT_ROOT} && {PROJECT_PY} analytics_plot.py",
    )

    # Luồng: Crawl → dbt → Archive → Analytics
    task_crawl >> task_dbt >> task_archive >> task_analytics
