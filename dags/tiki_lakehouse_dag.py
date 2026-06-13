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
    # so silver tables become queryable mid-crawl via Trino (and any BI tool
    # pointed at it) instead of waiting until the crawl finishes. The
    # downstream run_dbt uses trigger_rule=ALL_DONE, so a long or stuck crawl
    # no longer blocks marts.
    task_crawl = BashOperator(
        task_id='crawl_tiki_data',
        bash_command=f"cd {PROJECT_ROOT} && {PROJECT_PY} crawler/fetch_tiki.py",
        env={
            "TRIGGER_DBT_EVERY_N": "5",
            "DBT_BIN": PROJECT_DBT,
            "DBT_PROJECT_DIR": f"{PROJECT_ROOT}/dbt_tiki",
        },
        append_env=True,
    )

    # Task 2: rebuild staging + marts. `trigger_rule=ALL_DONE` means this runs
    # regardless of whether crawl succeeded, failed, or hit its timeout — so a
    # stuck crawl never blocks marts from refreshing on whatever bronze the
    # crawler did manage to write (plus the interleaved staging refreshes that
    # already populated silver mid-crawl).
    #
    # Partial-failure tolerance (the bash wrapper):
    #   dbt exit code 1 covers both "everything broke" AND "some models failed
    #   while others succeeded" — both look identical to BashOperator and would
    #   fail the task, which then SKIPs `archive_processed_bronze` and
    #   `generate_analytics_report` via their default ALL_SUCCESS trigger.
    #   We don't want that: if 5/10 models materialized fresh marts, archive
    #   and analytics should still run on those.
    #
    #   The wrapper parses dbt's summary line
    #     "Done. PASS=N WARN=M ERROR=K SKIP=L NO-OP=X TOTAL=Y"
    #   and:
    #     - exits 0 when PASS ≥ 1 (soft pass, downstream proceeds);
    #     - propagates the original exit code when PASS=0 (real failure,
    #       downstream SKIPs as expected).
    #   Real environment/auth/parse errors (no PASS line at all) also
    #   propagate, since the grep won't match.
    task_dbt = BashOperator(
        task_id='run_dbt',
        # DBT_TARGET_PATH ra ngoài bind mount (dbt_tiki/ bị mount từ host
        # Windows). Trước đây khi ta exec interactive `dbt clean/compile` để
        # debug, các file trong target/ được tạo dưới UID root, sau đó scheduled
        # DAG (chạy as airflow UID 50000) không overwrite được manifest.json
        # → PermissionError → toàn bộ dbt run abort. Ghi vào /tmp/dbt_target
        # vừa tránh xung đột UID, vừa thoáng hơn (tmpfs trong container).
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
            'so archive/analytics SKIP."; '
            'exit "$CODE"'
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

    # Task 5: Embed marts mới vào pgvector cho RAG chatbot. Chạy song song với
    # archive/analytics sau khi dbt hoàn tất — embedding là consumer độc lập
    # của lakehouse marts và không nên block archive.
    # `trigger_rule=ALL_DONE` khớp với `task_dbt`: nếu dbt partial-fail, embed
    # vẫn chạy trên marts hiện có (script idempotent qua content_hash, và có
    # guard "marts not yet materialized" tự exit 0).
    # DAG standalone `tiki_rag_indexer` (07:00) vẫn giữ làm backstop cho khi
    # main DAG fail hoàn toàn hoặc cho on-demand re-trigger sau `make crawl-cats`.
    # `python -u` + PYTHONUNBUFFERED để progress print không bị stdout buffer
    # giấu — nếu không, log dừng ở "Loading weights" và tưởng task treo.
    # OMP/MKL_NUM_THREADS=2 chừa CPU cho airflow worker gửi heartbeat — không
    # giới hạn thì PyTorch pin 100% core và scheduler nghĩ task chết → SIGTERM
    # giữa batch. RAG_INDEX_BATCH=4 giảm peak memory (BGE-M3 + batch 32 docs
    # ~4000 chars dễ vượt RAM Docker Desktop default → swap thrash 100x slow)
    # và flush print "upserted X/N" thường xuyên hơn.
    #
    # execution_timeout=240min nhắm vào first-time backfill: BGE-M3 trên CPU
    # ~4 doc/phút × ~650 product mới sau crawl rộng → ~2h45min. Daily runs
    # sau đó chỉ embed delta (content_hash dedup), thường xong trong vài phút,
    # nên cap 240min chỉ hiếm khi đụng. Nếu vẫn timeout: script idempotent,
    # `retries=1` (default_args) tự retry và tiếp tục từ chỗ bỏ dở.
    task_rag_index = BashOperator(
        task_id='rag_index_products',
        bash_command=f"cd {PROJECT_ROOT} && {PROJECT_PY} -u scripts/rag_index.py",
        env={
            "PYTHONUNBUFFERED": "1",
            "OMP_NUM_THREADS": "2",
            "MKL_NUM_THREADS": "2",
            "RAG_INDEX_BATCH": "4",
        },
        append_env=True,
        execution_timeout=timedelta(minutes=240),
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # Luồng: Crawl → dbt → Archive → Analytics
    #                  ↘ rag_index (parallel)
    task_crawl >> task_dbt >> task_archive >> task_analytics
    task_dbt >> task_rag_index
