"""Isolate the [32.0] BIGINT != VARCHAR binder failure step-by-step.

Run:
    docker exec tiki_airflow /opt/project-venv/bin/python /opt/project/crawler/_isolate_binder_bug.py
"""

import os

import duckdb

c = duckdb.connect()
c.execute("INSTALL httpfs; LOAD httpfs;")
c.execute(f"SET s3_endpoint='{os.getenv('S3_ENDPOINT_HOST', 'minio:9000')}';")
c.execute(f"SET s3_access_key_id='{os.getenv('MINIO_ACCESS_KEY', 'admin')}';")
c.execute(f"SET s3_secret_access_key='{os.getenv('MINIO_SECRET_KEY', 'minio_password')}';")
c.execute("SET s3_use_ssl=false; SET s3_url_style='path';")

bronze = os.getenv("BRONZE_BUCKET", "bronze")
pattern = f"s3://{bronze}/tiki_sellers/dt=*/run_id=*/*.parquet"

print("=== A. DESCRIBE post-migration unified schema ===")
print(
    c.execute(
        f"""
    DESCRIBE SELECT * FROM read_parquet(
        '{pattern}',
        hive_partitioning = FALSE,
        union_by_name = TRUE
    )
"""
    )
    .df()
    .to_string()
)
print()

print("=== B. Run the COMPLETE model SQL standalone (no dbt wrapping) ===")
try:
    rows = c.execute(
        f"""
        WITH raw AS (
            SELECT
                CAST(data AS VARCHAR) AS data,
                CAST(extracted_at AS VARCHAR) AS extracted_at,
                CAST(dt AS VARCHAR) AS dt,
                CAST(run_id AS VARCHAR) AS run_id
            FROM read_parquet(
                '{pattern}',
                hive_partitioning = FALSE,
                union_by_name = TRUE
            )
        ),
        flattened AS (
            SELECT
                dt,
                run_id,
                TRY_CAST(JSON_EXTRACT_STRING(data, '$.seller.id') AS BIGINT) AS seller_id,
                JSON_EXTRACT_STRING(data, '$.seller.name') AS seller_name,
                JSON_EXTRACT_STRING(data, '$.seller.slug') AS seller_slug,
                JSON_EXTRACT_STRING(data, '$.seller.logo') AS seller_logo,
                JSON_EXTRACT_STRING(data, '$.seller.icon') AS seller_icon,
                JSON_EXTRACT_STRING(data, '$.seller.store_level') AS store_level,
                TRY_CAST(JSON_EXTRACT_STRING(data, '$.seller.review_count') AS INTEGER) AS seller_review_count,
                TRY_CAST(JSON_EXTRACT_STRING(data, '$.seller.avg_rating_point') AS FLOAT) AS avg_rating_point,
                TRY_CAST(JSON_EXTRACT_STRING(data, '$.seller.total_follower') AS INTEGER) AS total_follower,
                TRY_CAST(JSON_EXTRACT_STRING(data, '$.seller.is_official') AS BOOLEAN) AS is_official,
                JSON_EXTRACT_STRING(data, '$.seller.url_key') AS url_key,
                STRPTIME(extracted_at, '%Y%m%d_%H%M%S') AS extracted_at_ts
            FROM raw
            WHERE data IS NOT NULL
        ),
        ranked AS (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY seller_id
                    ORDER BY extracted_at_ts DESC
                ) AS rn
            FROM flattened
            WHERE seller_id IS NOT NULL
        )
        SELECT COUNT(*) AS n FROM ranked WHERE rn = 1
    """
    ).df()
    print(f"OK — full pipeline returned {rows.iloc[0]['n']} rows")
except Exception as e:
    print(f"FAIL: {type(e).__name__}: {e}")
print()

print("=== C. Same pipeline but wrapped in CREATE OR REPLACE TABLE (mimics dbt) ===")
try:
    c.execute("DROP TABLE IF EXISTS test_stg_tiki_seller_info")
    c.execute(
        f"""
        CREATE TABLE test_stg_tiki_seller_info AS
        WITH raw AS (
            SELECT
                CAST(data AS VARCHAR) AS data,
                CAST(extracted_at AS VARCHAR) AS extracted_at,
                CAST(dt AS VARCHAR) AS dt,
                CAST(run_id AS VARCHAR) AS run_id
            FROM read_parquet(
                '{pattern}',
                hive_partitioning = FALSE,
                union_by_name = TRUE
            )
        ),
        flattened AS (
            SELECT
                dt, run_id,
                TRY_CAST(JSON_EXTRACT_STRING(data, '$.seller.id') AS BIGINT) AS seller_id,
                JSON_EXTRACT_STRING(data, '$.seller.name') AS seller_name,
                STRPTIME(extracted_at, '%Y%m%d_%H%M%S') AS extracted_at_ts
            FROM raw
            WHERE data IS NOT NULL
        ),
        ranked AS (
            SELECT *,
                ROW_NUMBER() OVER (PARTITION BY seller_id ORDER BY extracted_at_ts DESC) AS rn
            FROM flattened WHERE seller_id IS NOT NULL
        )
        SELECT * FROM ranked WHERE rn = 1
    """
    )
    print("OK — CREATE TABLE succeeded")
    print(c.execute("SELECT COUNT(*) FROM test_stg_tiki_seller_info").fetchone())
except Exception as e:
    print(f"FAIL: {type(e).__name__}: {e}")
print()

print("=== D. Same but with COPY TO parquet (exactly mimics dbt external materialization) ===")
try:
    c.execute(
        f"""
        COPY (
            WITH raw AS (
                SELECT
                    CAST(data AS VARCHAR) AS data,
                    CAST(extracted_at AS VARCHAR) AS extracted_at,
                    CAST(dt AS VARCHAR) AS dt,
                    CAST(run_id AS VARCHAR) AS run_id
                FROM read_parquet(
                    '{pattern}',
                    hive_partitioning = FALSE,
                    union_by_name = TRUE
                )
            ),
            flattened AS (
                SELECT
                    dt, run_id,
                    TRY_CAST(JSON_EXTRACT_STRING(data, '$.seller.id') AS BIGINT) AS seller_id,
                    JSON_EXTRACT_STRING(data, '$.seller.name') AS seller_name,
                    STRPTIME(extracted_at, '%Y%m%d_%H%M%S') AS extracted_at_ts
                FROM raw
                WHERE data IS NOT NULL
            )
            SELECT * FROM flattened LIMIT 5
        ) TO 's3://silver/test_stg_tiki_seller_info.parquet' (FORMAT PARQUET)
    """
    )
    print("OK — COPY TO parquet succeeded")
except Exception as e:
    print(f"FAIL: {type(e).__name__}: {e}")
