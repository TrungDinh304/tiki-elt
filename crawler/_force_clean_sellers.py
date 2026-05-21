"""Verify each tiki_sellers parquet's actual current schema and force-clean
any file still containing `new_version`. Uses pyarrow directly to bypass
pandas/MinIO quirks.

Run:
    docker exec tiki_airflow /opt/project-venv/bin/python /opt/project/crawler/_force_clean_sellers.py
"""
import json
import os
from io import BytesIO

import boto3
import pyarrow as pa
import pyarrow.parquet as pq

S3_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
S3_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
S3_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minio_password")
BRONZE_BUCKET = os.getenv("BRONZE_BUCKET", "bronze")
PREFIX = "tiki_sellers/"
KEEP = ["data", "extracted_at", "dt", "run_id"]

s3 = boto3.client(
    "s3",
    endpoint_url=S3_ENDPOINT,
    aws_access_key_id=S3_ACCESS_KEY,
    aws_secret_access_key=S3_SECRET_KEY,
)


def _force_string(arr):
    """Cast any arrow array to string, JSON-serializing nested values."""
    if pa.types.is_string(arr.type) or pa.types.is_large_string(arr.type):
        return arr
    if pa.types.is_struct(arr.type) or pa.types.is_list(arr.type):
        py_values = arr.to_pylist()
        return pa.array(
            [json.dumps(v, ensure_ascii=False, default=str) if v is not None else None for v in py_values],
            type=pa.string(),
        )
    return arr.cast(pa.string())


paginator = s3.get_paginator("list_objects_v2")
keys = []
for page in paginator.paginate(Bucket=BRONZE_BUCKET, Prefix=PREFIX):
    for obj in page.get("Contents", []):
        if obj["Key"].endswith(".parquet"):
            keys.append(obj["Key"])

print(f"Found {len(keys)} parquet files\n")

for key in keys:
    body = s3.get_object(Bucket=BRONZE_BUCKET, Key=key)["Body"].read()
    table = pq.read_table(BytesIO(body))
    cols_before = list(table.schema.names)
    print(f"BEFORE  {key}")
    print(f"        columns: {cols_before}")

    if set(cols_before) == set(KEEP):
        print(f"        already clean — skipping rewrite\n")
        continue

    # Build a clean arrow table with only KEEP columns, all as string
    arrays = []
    for col in KEEP:
        if col in cols_before:
            arrays.append(_force_string(table.column(col)))
        else:
            arrays.append(pa.nulls(table.num_rows, type=pa.string()))
    clean_table = pa.Table.from_arrays(arrays, names=KEEP)

    # Verify before upload
    print(f"        rewriting to: {list(clean_table.schema.names)}")

    buf = BytesIO()
    pq.write_table(clean_table, buf, compression="snappy")
    s3.put_object(Bucket=BRONZE_BUCKET, Key=key, Body=buf.getvalue())

    # Verify by re-downloading
    verify_body = s3.get_object(Bucket=BRONZE_BUCKET, Key=key)["Body"].read()
    verify_table = pq.read_table(BytesIO(verify_body))
    print(f"AFTER   verified columns: {list(verify_table.schema.names)}\n")

print("=== DONE ===")
print("Run the binder isolator again to confirm new_version is gone:")
print("  docker exec tiki_airflow /opt/project-venv/bin/python /opt/project/crawler/_isolate_binder_bug.py")
