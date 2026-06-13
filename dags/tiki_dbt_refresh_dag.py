"""Independent dbt refresh DAG.

Runs dbt (staging + marts) + analytics on a fixed cadence regardless of the
crawler. Reads whatever bronze/silver currently exists and rebuilds the
lakehouse marts. Use this when:
- the main `tiki_lakehouse_daily_pipeline` crawl gets stuck or fails and you
  still want fresh marts for Trino / external BI tools;
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
    # every day at 6am 
    schedule='0 6 * * *',
    start_date=datetime(2026, 5, 22),
    catchup=False,
    max_active_runs=1,
    tags=['tiki', 'lakehouse', 'dbt-only'],
) as dag:

    # See `dags/tiki_lakehouse_dag.py` for (a) why we wrap dbt: PASS≥1 → exit 0
    # so `generate_analytics_report` runs on whatever marts dbt did produce,
    # PASS=0 → propagate real failure so analytics SKIPs; (b) why DBT_TARGET_PATH
    # is forced to /tmp (avoid bind-mount UID conflicts on manifest.json).
    task_dbt = BashOperator(
        task_id='run_dbt',
        env={"DBT_TARGET_PATH": "/tmp/dbt_target"},
        append_env=True,
        bash_command=(
            f'cd {PROJECT_ROOT}/dbt_tiki || exit 1; '
            f'{PROJECT_DBT} run --profiles-dir . '
            '--vars "{bronze_bucket: bronze, silver_bucket: silver, lakehouse_bucket: lakehouse}" '
            '2>&1 | tee /tmp/dbt_run_$$.log; '
            'CODE=${PIPESTATUS[0]}; '
            'if [ "$CODE" -eq 0 ]; then exit 0; fi; '
            'if grep -qE "Done\\. PASS=[1-9][0-9]* WARN=" /tmp/dbt_run_$$.log; then '
            '  echo "[run_dbt] dbt partial-success — at least 1 model passed; '
            'downstream tasks will proceed."; '
            '  exit 0; '
            'fi; '
            'echo "[run_dbt] dbt produced zero passing models; propagating failure '
            'so analytics SKIPs."; '
            'exit "$CODE"'
        ),
        execution_timeout=timedelta(minutes=15),
    )

    task_analytics = BashOperator(
        task_id='generate_analytics_report',
        bash_command=f"cd {PROJECT_ROOT} && {PROJECT_PY} scripts/analytics_plot.py",
        execution_timeout=timedelta(minutes=5),
    )

    task_dbt >> task_analytics
