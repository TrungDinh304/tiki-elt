"""Independent dbt refresh DAG.

Runs dbt (staging + marts) + analytics on a fixed cadence regardless of the
crawler. Reads whatever bronze/silver currently exists and rebuilds the
lakehouse marts. Use this when:
- the main `tiki_lakehouse_daily_pipeline` crawl gets stuck or fails and you
  still want fresh marts for Superset/Trino;
- you want marts to refresh more often than daily without recrawling.

Does NOT include `archive_processed_bronze` — archiving is a one-shot operation
tied to a completed crawl and should stay in the main DAG.

Concurrency note: this DAG and the main DAG both write to the same
`tiki.duckdb` file. `max_active_runs=1` prevents this DAG from overlapping with
itself; if it fires while the main DAG's `run_dbt` is also executing, one of
them will fail on DuckDB's single-writer lock and retry. Acceptable trade-off
for now — schedule below is offset so collisions are unlikely.
"""
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from pathlib import Path

PROJECT_ROOT = str(Path(__file__).resolve().parent.parent)
PROJECT_PY = "/opt/project-venv/bin/python"
PROJECT_DBT = "/opt/project-venv/bin/dbt"

default_args = {
    'owner': 'tiki_admin',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    'tiki_dbt_refresh',
    default_args=default_args,
    description='Rebuild dbt staging+marts and analytics from existing bronze, every 2 hours',
    # Offset by 30 min from the main daily DAG (which runs at 0:00) so the
    # first 2h slot lands at 0:30, well after the daily crawl normally starts.
    schedule='30 */2 * * *',
    start_date=datetime(2026, 5, 22),
    catchup=False,
    max_active_runs=1,
    tags=['tiki', 'lakehouse', 'dbt-only'],
) as dag:

    task_dbt = BashOperator(
        task_id='run_dbt',
        bash_command=(
            f"cd {PROJECT_ROOT}/dbt_tiki && "
            f"{PROJECT_DBT} run --profiles-dir . "
            '--vars "{bronze_bucket: bronze, silver_bucket: silver, lakehouse_bucket: lakehouse}"'
        ),
        execution_timeout=timedelta(minutes=15),
    )

    task_analytics = BashOperator(
        task_id='generate_analytics_report',
        bash_command=f"cd {PROJECT_ROOT} && {PROJECT_PY} scripts/analytics_plot.py",
        execution_timeout=timedelta(minutes=5),
    )

    task_dbt >> task_analytics
