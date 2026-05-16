"""Move processed bronze partitions to the _processed/ prefix.

Run AFTER dbt has successfully consumed bronze data. The staging models read
from `s3://bronze/<entity>/dt=*/run_id=*/*.parquet`, so once those partitions
are moved under `_processed/`, subsequent dbt runs only see fresh crawl batches.
This is the historical-archive marker requested in the design.
"""
import os
import sys
import argparse
import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

load_dotenv()

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minio_password")
BRONZE_BUCKET = os.getenv("BRONZE_BUCKET", "bronze")

ENTITIES = ["tiki_products", "tiki_product_details", "tiki_sellers", "tiki_reviews"]


def _s3():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )


def list_keys(s3, prefix):
    paginator = s3.get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=BRONZE_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []) or []:
            keys.append(obj["Key"])
    return keys


def archive_entity(s3, entity, dt=None):
    """Move all dt= partitions (or just one when dt is given) under _processed/."""
    base_prefix = f"{entity}/"
    keys = list_keys(s3, base_prefix)

    # Filter: only live (non-processed) partitions
    live_keys = [k for k in keys if not k.startswith(f"{entity}/_processed/")]
    if dt:
        live_keys = [k for k in live_keys if f"/dt={dt}/" in k]

    if not live_keys:
        print(f"[{entity}] nothing to archive (dt={dt or '*'})")
        return 0

    moved = 0
    for src_key in live_keys:
        # src:  tiki_products/dt=2026-05-15/run_id=.../file.parquet
        # dst:  tiki_products/_processed/dt=2026-05-15/run_id=.../file.parquet
        suffix = src_key[len(base_prefix):]
        dst_key = f"{entity}/_processed/{suffix}"
        try:
            s3.copy_object(
                Bucket=BRONZE_BUCKET,
                CopySource={"Bucket": BRONZE_BUCKET, "Key": src_key},
                Key=dst_key,
            )
            s3.delete_object(Bucket=BRONZE_BUCKET, Key=src_key)
            moved += 1
        except ClientError as err:
            print(f"  failed to move {src_key}: {err}", file=sys.stderr)

    print(f"[{entity}] archived {moved} file(s) → _processed/")
    return moved


def main():
    parser = argparse.ArgumentParser(description="Archive processed bronze partitions.")
    parser.add_argument("--dt", help="Only archive this dt (YYYY-MM-DD). Default: all live partitions.")
    parser.add_argument("--entities", nargs="*", default=ENTITIES, help="Entities to archive.")
    args = parser.parse_args()

    s3 = _s3()
    total = 0
    for entity in args.entities:
        total += archive_entity(s3, entity, dt=args.dt)
    print(f"Done. Total files archived: {total}")


if __name__ == "__main__":
    main()
