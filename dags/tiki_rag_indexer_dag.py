"""RAG indexer DAG.

Reads `dim_products` + `fct_reviews` from the lakehouse, embeds each product
locally via sentence-transformers (BGE-M3), and upserts into pgvector. Runs
after the daily dbt refresh so embeddings reflect the latest marts.

Standalone backstop for the `rag_index_products` task that already lives in
`tiki_lakehouse_daily_pipeline`; useful for on-demand re-trigger after
`make crawl-cats` without re-running the full ELT.
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
        bash_command=f"cd {PROJECT_ROOT} && {PROJECT_PY} -u scripts/rag_index.py",
        env={
            "PYTHONUNBUFFERED": "1",
            # Xem comment cùng task ở dags/tiki_lakehouse_dag.py — giới hạn
            # CPU thread + batch nhỏ để tránh swap thrash và heartbeat timeout.
            # Timeout 240min đủ cho backfill ~650 sản phẩm fresh (~4 prod/min
            # trên CPU). Incremental daily run thường vài phút.
            "OMP_NUM_THREADS": "2",
            "MKL_NUM_THREADS": "2",
            "RAG_INDEX_BATCH": "4",
        },
        append_env=True,
        execution_timeout=timedelta(minutes=240),
    )
