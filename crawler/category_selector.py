"""Select which leaf categories to crawl on this run and track watermarks.

Strategy: pick the N leaf categories with the oldest `last_crawled_at` in
`s3://<bronze>/_state/category_watermark.json`. Categories never crawled
sort first (no entry → epoch 0). After a successful crawl the watermark
gets bumped to "now" so the rotation moves on.

Source of leaves, in order of preference:
  1. In-process cache (avoid redundant DuckDB queries within one run).
  2. `s3://<lakehouse>/marts/dim_categories.parquet` (dbt output).
  3. Raw `s3://<bronze>/tiki_categories/dt=*/run_id=*/*.parquet`.
  4. If all empty, run crawler/fetch_category.py to populate (3), then retry.
"""
import json
import os
from datetime import datetime, timezone

import boto3
import duckdb
from botocore.exceptions import ClientError

WATERMARK_KEY = "_state/category_watermark.json"
_EPOCH_ISO = "1970-01-01T00:00:00+00:00"

# In-process cache: avoid re-querying parquet on repeated calls within one run.
# Keyed by (bronze_bucket, lakehouse_bucket, endpoint) so different envs don't bleed.
_LEAVES_CACHE: dict = {}


def _now_iso():
    return datetime.now(timezone.utc).isoformat()


def _duckdb_with_s3(endpoint, access_key, secret_key):
    conn = duckdb.connect()
    conn.execute("INSTALL httpfs;")
    conn.execute("LOAD httpfs;")
    # endpoint comes in as full URL (http://localhost:9000); DuckDB wants host:port
    host = endpoint.replace("http://", "").replace("https://", "").rstrip("/")
    conn.execute(f"SET s3_endpoint='{host}';")
    conn.execute(f"SET s3_access_key_id='{access_key}';")
    conn.execute(f"SET s3_secret_access_key='{secret_key}';")
    conn.execute("SET s3_use_ssl=false;")
    conn.execute("SET s3_url_style='path';")
    return conn


def _query_leaves(conn, bronze_bucket, lakehouse_bucket):
    """Run lakehouse-first, bronze-fallback queries and return whichever has rows."""
    queries = [
        (
            "lakehouse",
            f"""SELECT category_id
                FROM read_parquet('s3://{lakehouse_bucket}/marts/dim_categories.parquet')
                WHERE is_leaf = TRUE AND category_id IS NOT NULL""",
        ),
        (
            "bronze",
            f"""SELECT DISTINCT TRY_CAST(category_id AS BIGINT) AS category_id
                FROM read_parquet(
                    's3://{bronze_bucket}/tiki_categories/dt=*/run_id=*/*.parquet',
                    hive_partitioning = TRUE, union_by_name = TRUE
                )
                WHERE is_leaf = TRUE AND category_id IS NOT NULL""",
        ),
    ]

    for source, q in queries:
        try:
            df = conn.execute(q).df()
            ids = [int(x) for x in df["category_id"].dropna().unique().tolist()]
            if ids:
                print(f"[category_selector] loaded {len(ids)} leaves from {source}")
                return ids
        except Exception as err:
            print(f"[category_selector] {source} query failed, trying fallback: {err}")
    return []


def _bootstrap_bronze_categories():
    """Trigger crawler/fetch_category.py when no leaves are available anywhere.

    Imported lazily so this module remains importable even if fetch_category
    has transient issues (e.g. network down during unit tests). The try/except
    covers both invocation styles: running `python crawler/fetch_tiki.py`
    (where `fetch_category` is on sys.path) and importing as a package
    (`from crawler import category_selector`).
    """
    print("[category_selector] no leaves found — running fetch_category to bootstrap…")
    try:
        from fetch_category import main as fetch_category_main
    except ImportError:
        from crawler.fetch_category import main as fetch_category_main
    fetch_category_main()


def clear_cache():
    """Reset the in-process leaves cache. Mostly useful for tests."""
    _LEAVES_CACHE.clear()


def load_leaf_categories(
    bronze_bucket, lakehouse_bucket, endpoint, access_key, secret_key,
    *, allow_bootstrap=True,
):
    """Return list[int] of leaf category_ids.

    Order of preference:
      1. In-process cache (avoid redundant DuckDB calls within one run).
      2. `dim_categories.parquet` in the lakehouse bucket.
      3. Raw `tiki_categories/dt=*/run_id=*/*.parquet` in bronze.
      4. As a last resort, run fetch_category to populate bronze and retry (3).

    Set `allow_bootstrap=False` to disable the network-fetch fallback (useful
    for tests and dry-runs).
    """
    cache_key = (bronze_bucket, lakehouse_bucket, endpoint)
    cached = _LEAVES_CACHE.get(cache_key)
    if cached is not None:
        return list(cached)

    conn = _duckdb_with_s3(endpoint, access_key, secret_key)
    ids = _query_leaves(conn, bronze_bucket, lakehouse_bucket)

    if not ids and allow_bootstrap:
        _bootstrap_bronze_categories()
        ids = _query_leaves(conn, bronze_bucket, lakehouse_bucket)

    _LEAVES_CACHE[cache_key] = list(ids)
    return ids


def _s3_client(endpoint, access_key, secret_key):
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )


def read_watermark(bronze_bucket, endpoint, access_key, secret_key):
    """Return {category_id(int): last_crawled_at(iso)} or {} on miss."""
    s3 = _s3_client(endpoint, access_key, secret_key)
    try:
        obj = s3.get_object(Bucket=bronze_bucket, Key=WATERMARK_KEY)
        data = json.loads(obj["Body"].read())
        return {int(k): v for k, v in data.get("watermark", {}).items()}
    except ClientError as err:
        if err.response["Error"]["Code"] in ("NoSuchKey", "404"):
            return {}
        raise


def write_watermark(bronze_bucket, watermark, endpoint, access_key, secret_key):
    """Persist the watermark map back to S3."""
    s3 = _s3_client(endpoint, access_key, secret_key)
    body = json.dumps(
        {
            "updated_at": _now_iso(),
            "watermark": {str(k): v for k, v in watermark.items()},
        },
        ensure_ascii=False,
        indent=2,
    )
    s3.put_object(Bucket=bronze_bucket, Key=WATERMARK_KEY, Body=body.encode("utf-8"))


def select_categories_to_crawl(leaves, watermark, budget):
    """Pick `budget` leaves with the oldest watermark first (None → epoch)."""
    if budget <= 0 or not leaves:
        return []
    ranked = sorted(leaves, key=lambda cid: watermark.get(cid, _EPOCH_ISO))
    return ranked[:budget]


def bump_watermark(watermark, category_ids):
    """Mutate `watermark` to set crawled_at=now for each id."""
    now = _now_iso()
    for cid in category_ids:
        watermark[int(cid)] = now
    return watermark


# Convenience wrapper bundling the three reads (env-driven defaults).
def plan_crawl(budget):
    bronze = os.getenv("BRONZE_BUCKET", "bronze")
    lakehouse = os.getenv("LAKEHOUSE_BUCKET", "lakehouse")
    endpoint = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
    ak = os.getenv("MINIO_ACCESS_KEY", "admin")
    sk = os.getenv("MINIO_SECRET_KEY", "minio_password")

    leaves = load_leaf_categories(bronze, lakehouse, endpoint, ak, sk)
    watermark = read_watermark(bronze, endpoint, ak, sk)
    chosen = select_categories_to_crawl(leaves, watermark, budget)
    return {
        "bronze": bronze,
        "endpoint": endpoint,
        "access_key": ak,
        "secret_key": sk,
        "leaves_total": len(leaves),
        "watermark": watermark,
        "chosen": chosen,
    }
