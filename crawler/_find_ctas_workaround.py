"""Find a SQL pattern that works with CREATE TABLE AS in DuckDB 1.10.1
for the seller staging model.

Run:
    docker exec tiki_airflow /opt/project-venv/bin/python /opt/project/crawler/_find_ctas_workaround.py
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


def try_ctas(label, sql):
    print(f"--- {label} ---")
    try:
        c.execute("DROP TABLE IF EXISTS t_workaround")
        c.execute(f"CREATE TABLE t_workaround AS {sql}")
        n = c.execute("SELECT COUNT(*) FROM t_workaround").fetchone()[0]
        print(f"  PASS — {n} rows")
        return True
    except Exception as e:
        print(f"  FAIL — {type(e).__name__}: {str(e).splitlines()[0]}")
        return False


# Variant 1: original ranked CTE with SELECT * + rn
v1 = f"""
WITH raw AS (
    SELECT
        CAST(data AS VARCHAR) AS data,
        CAST(extracted_at AS VARCHAR) AS extracted_at,
        CAST(dt AS VARCHAR) AS dt,
        CAST(run_id AS VARCHAR) AS run_id
    FROM read_parquet('{pattern}', hive_partitioning=FALSE, union_by_name=TRUE)
),
flattened AS (
    SELECT dt, run_id,
        TRY_CAST(JSON_EXTRACT_STRING(data, '$.seller.id') AS BIGINT) AS seller_id,
        JSON_EXTRACT_STRING(data, '$.seller.name') AS seller_name,
        STRPTIME(extracted_at, '%Y%m%d_%H%M%S') AS extracted_at_ts
    FROM raw WHERE data IS NOT NULL
),
ranked AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY seller_id ORDER BY extracted_at_ts DESC) AS rn
    FROM flattened WHERE seller_id IS NOT NULL
)
SELECT * FROM ranked WHERE rn = 1
"""

# Variant 2: explicit columns instead of SELECT *
v2 = f"""
WITH raw AS (
    SELECT
        CAST(data AS VARCHAR) AS data,
        CAST(extracted_at AS VARCHAR) AS extracted_at,
        CAST(dt AS VARCHAR) AS dt,
        CAST(run_id AS VARCHAR) AS run_id
    FROM read_parquet('{pattern}', hive_partitioning=FALSE, union_by_name=TRUE)
),
flattened AS (
    SELECT dt, run_id,
        TRY_CAST(JSON_EXTRACT_STRING(data, '$.seller.id') AS BIGINT) AS seller_id,
        JSON_EXTRACT_STRING(data, '$.seller.name') AS seller_name,
        STRPTIME(extracted_at, '%Y%m%d_%H%M%S') AS extracted_at_ts
    FROM raw WHERE data IS NOT NULL
),
ranked AS (
    SELECT
        dt, run_id, seller_id, seller_name, extracted_at_ts,
        ROW_NUMBER() OVER (PARTITION BY seller_id ORDER BY extracted_at_ts DESC) AS rn
    FROM flattened WHERE seller_id IS NOT NULL
)
SELECT dt, run_id, seller_id, seller_name, extracted_at_ts
FROM ranked WHERE rn = 1
"""

# Variant 3: QUALIFY (no rn column at all)
v3 = f"""
WITH raw AS (
    SELECT
        CAST(data AS VARCHAR) AS data,
        CAST(extracted_at AS VARCHAR) AS extracted_at,
        CAST(dt AS VARCHAR) AS dt,
        CAST(run_id AS VARCHAR) AS run_id
    FROM read_parquet('{pattern}', hive_partitioning=FALSE, union_by_name=TRUE)
),
flattened AS (
    SELECT dt, run_id,
        TRY_CAST(JSON_EXTRACT_STRING(data, '$.seller.id') AS BIGINT) AS seller_id,
        JSON_EXTRACT_STRING(data, '$.seller.name') AS seller_name,
        STRPTIME(extracted_at, '%Y%m%d_%H%M%S') AS extracted_at_ts
    FROM raw WHERE data IS NOT NULL
)
SELECT dt, run_id, seller_id, seller_name, extracted_at_ts
FROM flattened
WHERE seller_id IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY seller_id ORDER BY extracted_at_ts DESC) = 1
"""

# Variant 4: DISTINCT ON
v4 = f"""
WITH raw AS (
    SELECT
        CAST(data AS VARCHAR) AS data,
        CAST(extracted_at AS VARCHAR) AS extracted_at,
        CAST(dt AS VARCHAR) AS dt,
        CAST(run_id AS VARCHAR) AS run_id
    FROM read_parquet('{pattern}', hive_partitioning=FALSE, union_by_name=TRUE)
),
flattened AS (
    SELECT dt, run_id,
        TRY_CAST(JSON_EXTRACT_STRING(data, '$.seller.id') AS BIGINT) AS seller_id,
        JSON_EXTRACT_STRING(data, '$.seller.name') AS seller_name,
        STRPTIME(extracted_at, '%Y%m%d_%H%M%S') AS extracted_at_ts
    FROM raw WHERE data IS NOT NULL
)
SELECT DISTINCT ON (seller_id) dt, run_id, seller_id, seller_name, extracted_at_ts
FROM flattened
WHERE seller_id IS NOT NULL
ORDER BY seller_id, extracted_at_ts DESC
"""

# Variant 5: materialize via SELECT to intermediate then CTAS from it
v5_setup = f"""
CREATE OR REPLACE TEMP TABLE raw_data AS
SELECT
    CAST(data AS VARCHAR) AS data,
    CAST(extracted_at AS VARCHAR) AS extracted_at,
    CAST(dt AS VARCHAR) AS dt,
    CAST(run_id AS VARCHAR) AS run_id
FROM read_parquet('{pattern}', hive_partitioning=FALSE, union_by_name=TRUE)
"""
v5 = """
WITH flattened AS (
    SELECT dt, run_id,
        TRY_CAST(JSON_EXTRACT_STRING(data, '$.seller.id') AS BIGINT) AS seller_id,
        JSON_EXTRACT_STRING(data, '$.seller.name') AS seller_name,
        STRPTIME(extracted_at, '%Y%m%d_%H%M%S') AS extracted_at_ts
    FROM raw_data WHERE data IS NOT NULL
),
ranked AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY seller_id ORDER BY extracted_at_ts DESC) AS rn
    FROM flattened WHERE seller_id IS NOT NULL
)
SELECT * FROM ranked WHERE rn = 1
"""

try_ctas("V1: original ranked CTE + SELECT *", v1)
try_ctas("V2: explicit columns in ranked + final SELECT", v2)
try_ctas("V3: QUALIFY (no rn column)", v3)
try_ctas("V4: DISTINCT ON", v4)
c.execute(v5_setup)
try_ctas("V5: pre-materialized raw temp table", v5)
