"""One-off migration: normalize legacy bronze/tiki_sellers/*.parquet to the
4-column strict schema (data, extracted_at, dt, run_id).

Run inside the airflow container:
    docker exec tiki_airflow /opt/project-venv/bin/python /opt/project/crawler/_migrate_seller_schema.py
"""

import json
import os
from io import BytesIO

import boto3
import pandas as pd

S3_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
S3_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
S3_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minio_password")
BRONZE_BUCKET = os.getenv("BRONZE_BUCKET", "bronze")
PREFIX = "tiki_sellers/"
KEEP_COLS = ["data", "extracted_at", "dt", "run_id"]

s3 = boto3.client(
    "s3",
    endpoint_url=S3_ENDPOINT,
    aws_access_key_id=S3_ACCESS_KEY,
    aws_secret_access_key=S3_SECRET_KEY,
)


def _as_json_string(value):
    if value is None:
        return None
    if isinstance(value, str):
        return value
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False, default=str)
    return str(value)


def migrate_file(key: str) -> dict:
    body = s3.get_object(Bucket=BRONZE_BUCKET, Key=key)["Body"].read()
    df = pd.read_parquet(BytesIO(body))
    original_cols = list(df.columns)
    rows_before = len(df)

    available = [c for c in KEEP_COLS if c in df.columns]
    if not available:
        return {"key": key, "skipped": "no useful columns", "rows": rows_before}

    df = df[available].copy()
    if "data" in df.columns:
        df["data"] = df["data"].apply(_as_json_string)
    for col in ("extracted_at", "dt", "run_id"):
        if col in df.columns:
            df[col] = df[col].astype("string")

    buf = BytesIO()
    df.to_parquet(buf, index=False)
    s3.put_object(Bucket=BRONZE_BUCKET, Key=key, Body=buf.getvalue())

    dropped = [c for c in original_cols if c not in KEEP_COLS]
    return {
        "key": key,
        "rows": rows_before,
        "kept": available,
        "dropped": dropped,
    }


def main():
    paginator = s3.get_paginator("list_objects_v2")
    stats = {"migrated": 0, "skipped": 0, "errors": 0}
    keys_seen = []
    for page in paginator.paginate(Bucket=BRONZE_BUCKET, Prefix=PREFIX):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            keys_seen.append(key)
            if not key.endswith(".parquet"):
                continue
            try:
                result = migrate_file(key)
                if "skipped" in result:
                    stats["skipped"] += 1
                    print(f"SKIP {key}: {result['skipped']}")
                else:
                    stats["migrated"] += 1
                    if result["dropped"]:
                        print(f"OK   {key} rows={result['rows']} " f"dropped={result['dropped']}")
                    else:
                        print(f"OK   {key} rows={result['rows']} (no-op)")
            except Exception as err:
                stats["errors"] += 1
                print(f"ERR  {key}: {type(err).__name__}: {err}")

    print()
    print(
        f"=== Summary: migrated={stats['migrated']} "
        f"skipped={stats['skipped']} errors={stats['errors']} "
        f"keys_listed={len(keys_seen)} ==="
    )


if __name__ == "__main__":
    main()
