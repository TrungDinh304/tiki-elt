from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime, timedelta

# Đường dẫn tuyệt đối tới thư mục chứa project
PROJECT_ROOT = "/home/tunguyenn99/my-project/tiki-lakehouse"

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

    # Task 1: Chạy crawler
    # uv run tự động sử dụng môi trường của bạn để cài đặt dependencies
    task_crawl = BashOperator(
        task_id='crawl_tiki_data',
        bash_command=f"cd {PROJECT_ROOT} && uv run python crawler/fetch_tiki.py",
    )

    # Task 2: Chạy dbt transform
    # Lệnh make dbt-run sẽ dọn dẹp và chạy dbt
    task_dbt = BashOperator(
        task_id='run_dbt_transformation',
        bash_command=f"cd {PROJECT_ROOT} && make dbt-run",
    )

    # Task 3: Archive bronze data đã được dbt xử lý → _processed/
    # Chỉ chạy SAU khi dbt run thành công để tránh mất data nếu transform fail.
    task_archive = BashOperator(
        task_id='archive_processed_bronze',
        bash_command=f"cd {PROJECT_ROOT} && uv run python crawler/archive_processed.py",
    )

    # Task 4: Chạy script analytics để vẽ biểu đồ mới
    task_analytics = BashOperator(
        task_id='generate_analytics_report',
        bash_command=f"cd {PROJECT_ROOT} && uv run python analytics_plot.py",
    )

    # Luồng: Crawl → dbt → Archive → Analytics
    task_crawl >> task_dbt >> task_archive >> task_analytics
