"""Deeper diagnostic: ask DuckDB what types it actually sees per file.

Run:
    docker exec tiki_airflow /opt/project-venv/bin/python /opt/project/crawler/_check_seller_types.py
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

print("=== 1. DESCRIBE unified read (what DuckDB resolves columns to) ===")
df = c.execute(
    f"""
    DESCRIBE SELECT * FROM read_parquet(
        '{pattern}',
        hive_partitioning = FALSE,
        union_by_name = TRUE
    )
"""
).df()
print(df.to_string())
print()

print("=== 2. typeof(data) per file ===")
df = c.execute(
    f"""
    SELECT filename, typeof(data) AS t, COUNT(*) AS rows
    FROM read_parquet(
        '{pattern}',
        hive_partitioning = FALSE,
        union_by_name = TRUE,
        filename = TRUE
    )
    GROUP BY filename, t
    ORDER BY filename
"""
).df()
print(df.to_string(max_colwidth=120))
print()

print("=== 3. Per-file independent read (no union) ===")
files = (
    c.execute(
        f"""
    SELECT DISTINCT file_name FROM parquet_schema('{pattern}')
"""
    )
    .df()["file_name"]
    .tolist()
)

for f in files:
    print(f"\n--- {f} ---")
    schema = c.execute(
        f"""
        DESCRIBE SELECT * FROM read_parquet('{f}', hive_partitioning = FALSE)
    """
    ).df()
    print(schema.to_string())

print()
print("=== 4. Try the actual model raw CTE in isolation ===")
try:
    rows = c.execute(
        f"""
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
        LIMIT 5
    """
    ).df()
    print("OK — raw CTE returns:")
    print(rows.to_string(max_colwidth=80))
except Exception as e:
    print(f"FAIL: {type(e).__name__}: {e}")

print()
print("=== 5. Try the full flattened+ranked pipeline ===")
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
                STRPTIME(extracted_at, '%Y%m%d_%H%M%S') AS extracted_at_ts
            FROM raw
            WHERE data IS NOT NULL
        )
        SELECT COUNT(*) AS n FROM flattened
    """
    ).df()
    print(f"OK — flattened count: {rows.iloc[0]['n']}")
except Exception as e:
    print(f"FAIL: {type(e).__name__}: {e}")
