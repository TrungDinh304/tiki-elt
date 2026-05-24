"""RAG indexer DAG.

Reads `dim_products` + `fct_reviews` from the lakehouse, embeds each product
via the ds2api OpenAI-compatible endpoint, and upserts into pgvector. Runs
after the daily dbt refresh so embeddings reflect the latest marts.

Decoupled from `tiki_lakehouse_daily_pipeline` on purpose — embedding is an
optional downstream consumer of the lakehouse, and a ds2api outage should
not retro-fail the main ELT.
"""
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from pathlib import Path

PROJECT_ROOT = str(Path(__file__).resolve().parent.parent)
PROJECT_PY = "/opt/project-venv/bin/python"

default_args = {
    "owner": "tiki_admin",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "tiki_rag_indexer",
    default_args=default_args,
    description="Embed lakehouse products into pgvector for the RAG chatbot",
    # Runs 1h after tiki_dbt_refresh (06:00) so marts are guaranteed fresh.
    schedule="0 7 * * *",
    start_date=datetime(2026, 5, 22),
    catchup=False,
    max_active_runs=1,
    tags=["tiki", "rag", "chatbot"],
) as dag:

    task_index = BashOperator(
        task_id="rag_index_products",
        bash_command=f"cd {PROJECT_ROOT} && {PROJECT_PY} scripts/rag_index.py",
        execution_timeout=timedelta(minutes=30),
    )
