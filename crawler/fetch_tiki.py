import time
import os
import json
import random
import boto3
import requests
import pandas as pd
from datetime import datetime
from io import BytesIO
from dotenv import load_dotenv

# import redis\redis.py
# Use the local Redis helper in this repo (redis/redis.py)
# NOTE: `redis/` folder is not a Python package, so we load it by path.
import importlib.util
from pathlib import Path

_redis_helper_path = Path(__file__).resolve().parent.parent / "redis" / "redis.py"
_spec = importlib.util.spec_from_file_location("tiki_redis_helper", _redis_helper_path)
rd = importlib.util.module_from_spec(_spec)  # type: ignore
assert _spec and _spec.loader
try:
    _spec.loader.exec_module(rd)  # type: ignore
    # If Redis/PyPI redis client is not installed (or misconfigured), disable caching gracefully.
    try:
        _ = rd.cache  # type: ignore[attr-defined]
    except Exception:
        rd = None  # type: ignore
except Exception:
    # Don't crash crawler when redis client isn't available; just skip caching.
    rd = None  # type: ignore


load_dotenv()

# Tiki usually caps listings at 50 pages
DEFAULT_NUM_PAGES = 10  # Default 10 pages (~400 books)

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minio_password")
BRONZE_BUCKET = os.getenv("BRONZE_BUCKET", "bronze")
MAX_PRODUCT_REVIEWS = int(os.getenv("MAX_PRODUCT_REVIEWS", "20"))
MAX_REVIEW_PAGES = int(os.getenv("MAX_REVIEW_PAGES", "1"))
REQUEST_DELAY_MIN = float(os.getenv("REQUEST_DELAY_MIN", "1.5"))
REQUEST_DELAY_MAX = float(os.getenv("REQUEST_DELAY_MAX", "3.5"))

# How many leaf categories to crawl per run, and how deep per category.
# Tuned so a single run finishes in ~15min at the default delay.
BUDGET_PER_RUN = int(os.getenv("BUDGET_PER_RUN", "50"))
PAGES_PER_CATEGORY = int(os.getenv("PAGES_PER_CATEGORY", "5"))

# Micro-batch: after every N categories crawled, fire a dbt staging refresh
# so silver tables (and anything querying them through Trino) see fresh data
# without waiting for the whole run to finish. Set to 0 to disable.
TRIGGER_DBT_EVERY_N = int(os.getenv("TRIGGER_DBT_EVERY_N", "0"))
DBT_PROJECT_DIR = os.getenv(
    "DBT_PROJECT_DIR", str(Path(__file__).resolve().parent.parent / "dbt_tiki")
)
DBT_BIN = os.getenv("DBT_BIN", "dbt")
DBT_STAGING_SELECTOR = os.getenv("DBT_STAGING_SELECTOR", "path:models/staging")

_RUN_TS = datetime.now()
RUN_ID = _RUN_TS.strftime("%Y%m%d_%H%M%S")
RUN_DT = _RUN_TS.strftime("%Y-%m-%d")

SESSION = requests.Session()
# Tiki's listings endpoint started rejecting requests without a full browser-like
# header set (returns HTTP 400). These mimic a Chrome session on tiki.vn.
SESSION.headers.update(
    {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "vi,en-US;q=0.9,en;q=0.8",
        "Referer": "https://tiki.vn/",
        "Origin": "https://tiki.vn",
        "x-guest-token": "",
    }
)


def fetch_json(url, params=None, max_retries=3):
    for attempt in range(1, max_retries + 1):
        try:
            response = SESSION.get(url, params=params, timeout=15)
            if response.status_code == 200:
                return response.json()
            print(f"Request failed ({response.status_code}) for {url}")
        except requests.RequestException as err:
            print(f"Request exception for {url}: {err}")

        if attempt < max_retries:
            delay = min(REQUEST_DELAY_MIN * attempt, REQUEST_DELAY_MAX) + random.uniform(0, 0.5)
            print(f"Retrying in {delay:.1f}s...")
            time.sleep(delay)

    print(f"Giving up on {url}")
    return None


def get_delay():
    return random.uniform(REQUEST_DELAY_MIN, REQUEST_DELAY_MAX)


def fetch_products_for_category(category_id, num_pages=PAGES_PER_CATEGORY):
    """Fetch up to `num_pages` listing pages filtered by a single category_id.

    The Tiki listings endpoint started returning HTTP 400 without `category`,
    so this is now the only supported entry point.
    """
    all_products = []
    page_tag = f"cat{category_id}"

    for page in range(1, num_pages + 1):
        api_url = (
            "https://tiki.vn/api/personalish/v1/blocks/listings?"
            f"limit=40&include=advertisement&aggregations=2&"
            f"category={category_id}&page={page}"
        )
        print(f"[cat {category_id}] page {page}: {api_url}")

        response = fetch_json(api_url)
        if not response:
            break

        data = response.get("data", [])
        if not data:
            print(f"[cat {category_id}] no more data at page {page}")
            break

        # Test/Mock mode kept for crawler/tests/test_fetch.py.
        if isinstance(data, list) and len(data) == 1:
            all_products.extend(data)
            return all_products

        # Stamp every row with the category slot it was crawled under so bronze
        # readers can attribute products back to the rotation budget.
        for row in data:
            if isinstance(row, dict):
                row["crawl_category_id"] = int(category_id)

        print(f"[cat {category_id}] fetched {len(data)} products on page {page}")
        process_page(data, f"{page_tag}_p{page}")
        all_products.extend(data)

        time.sleep(get_delay())

    return all_products


_INTERLEAVED_DBT_STATS = {"ok": 0, "failed": 0}


def _run_dbt_staging_refresh():
    """Fire `dbt run` against the staging path so silver layer is fresh
    mid-crawl. Non-fatal: a failure here doesn't abort the rest of the crawl
    (we'd rather keep collecting raw data than stop because dbt blipped). The
    final dbt task in the DAG acts as a backstop for any failed refresh."""
    import subprocess
    import shutil

    # Resolve to an absolute path up front so PermissionError on the PATH
    # lookup turns into a clear log line instead of a crash.
    resolved = DBT_BIN if os.path.isabs(DBT_BIN) else shutil.which(DBT_BIN)
    if not resolved or not os.access(resolved, os.X_OK):
        _INTERLEAVED_DBT_STATS["failed"] += 1
        print(
            f"[interleaved-dbt] FAILED: dbt binary not executable "
            f"(DBT_BIN={DBT_BIN!r}, resolved={resolved!r}). "
            f"Set DBT_BIN to an absolute path like /opt/project-venv/bin/dbt."
        )
        return

    bronze = os.getenv("BRONZE_BUCKET", "bronze")
    silver = os.getenv("SILVER_BUCKET", "silver")
    lakehouse = os.getenv("LAKEHOUSE_BUCKET", "lakehouse")
    cmd = [
        resolved,
        "run",
        "--profiles-dir",
        ".",
        "--select",
        DBT_STAGING_SELECTOR,
        "--vars",
        f"{{bronze_bucket: {bronze}, silver_bucket: {silver}, lakehouse_bucket: {lakehouse}}}",
    ]
    print(f"[interleaved-dbt] launching: {' '.join(cmd)} (cwd={DBT_PROJECT_DIR})")
    try:
        subprocess.run(cmd, cwd=DBT_PROJECT_DIR, check=True, timeout=300)
        _INTERLEAVED_DBT_STATS["ok"] += 1
        print("[interleaved-dbt] staging refresh OK")
    except (
        subprocess.CalledProcessError,
        subprocess.TimeoutExpired,
        FileNotFoundError,
        PermissionError,
        OSError,
    ) as err:
        _INTERLEAVED_DBT_STATS["failed"] += 1
        print(
            f"[interleaved-dbt] FAILED: {type(err).__name__}: {err}. "
            "See dbt output above for the underlying cause. "
            "The final dbt task in the DAG will retry as a backstop."
        )


def fetch_products(category_ids, num_pages=PAGES_PER_CATEGORY):
    """Crawl multiple categories sequentially. If TRIGGER_DBT_EVERY_N > 0,
    a dbt staging refresh runs every N categories so silver is queryable
    before the whole batch finishes. The final partial batch (when the
    category count isn't a multiple of N) also gets refreshed so silver
    isn't stuck on stale data at the tail of the crawl."""
    combined = []
    total = len(category_ids)
    for idx, cid in enumerate(category_ids, start=1):
        combined.extend(fetch_products_for_category(cid, num_pages=num_pages))

        if TRIGGER_DBT_EVERY_N > 0 and (idx % TRIGGER_DBT_EVERY_N == 0 or idx == total):
            _run_dbt_staging_refresh()

    if TRIGGER_DBT_EVERY_N > 0:
        print(
            f"[interleaved-dbt] summary: ok={_INTERLEAVED_DBT_STATS['ok']} "
            f"failed={_INTERLEAVED_DBT_STATS['failed']}"
        )

    return combined


def fetch_product_details(product_id):
    if not product_id:
        return None

    url = f"https://tiki.vn/api/v2/products/{product_id}?platform=web"
    print(f"Fetching product details for {product_id}")
    return fetch_json(url)


def fetch_seller_info(seller_id):
    if not seller_id:
        return None

    url = "https://api.tiki.vn/product-detail/v2/widgets/seller"
    params = {"seller_id": str(seller_id)}

    print(f"Fetching seller info for seller_id={params['seller_id']}")

    if rd is not None and hasattr(rd, "get_data_from_api"):
        try:
            # If Redis cache is available, return None on cache hit to avoid unnecessary API calls.
            # Seller info is less critical than product details.
            cached_result = rd.get_data_from_api(url, params=params)  # type: ignore[attr-defined]
            if cached_result is not None:
                print(f"Seller info for seller_id={params['seller_id']} retrieved from cache.")
                return None
        except Exception as err:
            print(f"Redis cache helper failed: {err}")
            return fetch_json(url, params=params)

    return fetch_json(url, params=params)


def fetch_product_reviews(product_id, seller_id, page=1, limit=MAX_PRODUCT_REVIEWS):
    if not product_id or not seller_id:
        return None

    url = "https://tiki.vn/api/v2/reviews"
    params = {
        "limit": limit,
        "include": "comments,contribute_info,attribute_vote_summary",
        "sort": "score|desc,id|desc,stars|all",
        "page": page,
        "product_id": product_id,
        "seller_id": seller_id,
    }
    print(f"Fetching reviews for product_id={product_id}, seller_id={seller_id}, page={page}")
    return fetch_json(url, params=params)


def _json_default(value):
    if isinstance(value, (set, tuple)):
        return list(value)
    return str(value)


def sanitize_dataframe(df):
    """Serialize nested dict/list columns as valid JSON so silver layer
    can parse them with json_extract (single-quoted Python repr cannot)."""
    for col in df.columns:
        if df[col].apply(lambda x: isinstance(x, (dict, list))).any():
            df[col] = df[col].apply(
                lambda x: (
                    json.dumps(x, ensure_ascii=False, default=_json_default)
                    if isinstance(x, (dict, list))
                    else x
                )
            )
    return df


def save_to_minio(data, entity, file_name, preview=False):
    f"""Write a batch to bronze layer using Hive-style partitioning:
       s3://{BRONZE_BUCKET}/<entity>/dt=YYYY-MM-DD/run_id=YYYYMMDD_HHMMSS/<file_name>.parquet"""
    if not data:
        print(f"No data to save for {entity}")
        return

    df = pd.DataFrame(data)
    df["extracted_at"] = RUN_ID
    df["dt"] = RUN_DT
    df["run_id"] = RUN_ID
    df = sanitize_dataframe(df)

    if preview:
        preview_dir = "preview_data"
        os.makedirs(preview_dir, exist_ok=True)
        preview_path = os.path.join(preview_dir, f"{entity}_preview_{RUN_ID}.csv")
        df.to_csv(preview_path, index=False)
        print(f"Local CSV preview saved to: {preview_path}")

    parquet_buffer = BytesIO()
    df.to_parquet(parquet_buffer, index=False)

    s3_client = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )

    file_key = f"{entity}/dt={RUN_DT}/run_id={RUN_ID}/{file_name}.parquet"
    print(f"Uploading {len(df)} records to {BRONZE_BUCKET}/{file_key}")
    s3_client.put_object(Bucket=BRONZE_BUCKET, Key=file_key, Body=parquet_buffer.getvalue())
    print("Upload complete.")


def process_page(products, page_number):
    if not products:
        return

    page_prefix = f"page_{page_number}"
    save_to_minio(products, "tiki_products", f"books_{page_prefix}", preview=False)

    product_ids = []
    seller_ids = []
    seller_map = {}
    review_candidates = []

    for product in products:
        product_id = product.get("id") or product.get("product_id")
        seller_id_raw = product.get("seller_id") or product.get("seller", {}).get("id")
        seller_id = str(seller_id_raw) if seller_id_raw is not None else None
        review_count = product.get("review_count")

        if product_id:
            product_ids.append(product_id)
        if seller_id and seller_id not in seller_ids:
            seller_ids.append(seller_id)
        if product_id and seller_id:
            seller_map[product_id] = seller_id

        if product_id and seller_id and review_count is not None:
            try:
                review_count_value = int(review_count)
            except (TypeError, ValueError):
                review_count_value = 0

            if review_count_value > 0:
                review_candidates.append((product_id, seller_id))

    details = []
    for product_id in product_ids:
        detail = fetch_product_details(product_id)
        if detail:
            details.append(detail)
        time.sleep(get_delay())
    save_to_minio(details, "tiki_product_details", f"details_{page_prefix}")

    sellers = []
    for seller_id in seller_ids:
        info = fetch_seller_info(seller_id)
        # Tiki's seller widget response includes optional top-level keys
        # (ad/meta/error_*) that vary per seller. Letting them leak into the
        # DataFrame makes pandas infer different parquet types per batch
        # (BIGINT in one file, VARCHAR in the next), which then breaks
        # DuckDB's `union_by_name = TRUE` binder in stg_tiki_seller_info with
        # an INTERNAL assertion failure. Keep only the `data` field that
        # staging actually consumes so bronze always has a fixed schema.
        if isinstance(info, dict):
            seller_data = info.get("data")
            if seller_data:
                sellers.append({"data": seller_data})
        time.sleep(get_delay())
    save_to_minio(sellers, "tiki_sellers", f"sellers_{page_prefix}")

    reviews = []
    seen_reviews = set()
    for product_id, seller_id in review_candidates:
        if not product_id or not seller_id:
            continue
        if (product_id, seller_id) in seen_reviews:
            continue
        seen_reviews.add((product_id, seller_id))

        for review_page_num in range(1, MAX_REVIEW_PAGES + 1):
            review_page = fetch_product_reviews(product_id, seller_id, page=review_page_num)
            if review_page:
                review_page["product_id"] = product_id
                review_page["seller_id"] = seller_id
                review_page["page"] = review_page_num
                reviews.append(review_page)
            time.sleep(get_delay())
    save_to_minio(reviews, "tiki_reviews", f"reviews_{page_prefix}")


def write_success_marker():
    f"""Write a manifest at s3://{BRONZE_BUCKET}/_manifests/<dt>/<run_id>.json so
       downstream jobs can detect a completed crawl and dbt has a deterministic
       partition watermark to filter on."""
    s3_client = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )
    body = json.dumps({"dt": RUN_DT, "run_id": RUN_ID, "completed_at": datetime.now().isoformat()})
    key = f"_manifests/dt={RUN_DT}/{RUN_ID}.json"
    s3_client.put_object(Bucket=BRONZE_BUCKET, Key=key, Body=body)
    print(f"Manifest written: {BRONZE_BUCKET}/{key}")


if __name__ == "__main__":
    from category_selector import (
        plan_crawl,
        bump_watermark,
        read_watermark,
        write_watermark,
    )

    # On-demand override: when CRAWL_CATEGORY_IDS is set (comma-separated leaf
    # ids), skip the watermark rotation and crawl exactly those categories.
    # Used by `make crawl-cats IDS=...` to fill specific gaps in coverage
    # (e.g. add the Sách category to the RAG index without waiting for the
    # daily rotation to reach it).
    override = os.getenv("CRAWL_CATEGORY_IDS", "").strip()
    if override:
        chosen = [int(x) for x in override.split(",") if x.strip()]
        bronze = os.getenv("BRONZE_BUCKET", "bronze")
        endpoint = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
        ak = os.getenv("MINIO_ACCESS_KEY", "admin")
        sk = os.getenv("MINIO_SECRET_KEY", "minio_password")
        plan = {
            "bronze": bronze,
            "endpoint": endpoint,
            "access_key": ak,
            "secret_key": sk,
            "watermark": read_watermark(bronze, endpoint, ak, sk),
            "chosen": chosen,
        }
        print(
            f"On-demand crawl: {len(chosen)} categories = {chosen} "
            f"(pages={PAGES_PER_CATEGORY}, bypassing watermark rotation)"
        )
    else:
        plan = plan_crawl(BUDGET_PER_RUN)
        print(
            f"Found {plan['leaves_total']} leaf categories; "
            f"crawling {len(plan['chosen'])} this run "
            f"(budget={BUDGET_PER_RUN}, pages={PAGES_PER_CATEGORY})"
        )
    if not plan["chosen"]:
        raise SystemExit(
            "No leaf categories available. Run crawler/fetch_category.py first "
            "to populate s3://bronze/tiki_categories/."
        )

    products = fetch_products(plan["chosen"], num_pages=PAGES_PER_CATEGORY)
    if not products:
        raise SystemExit("No products fetched — refusing to write success manifest")

    bump_watermark(plan["watermark"], plan["chosen"])
    write_watermark(
        plan["bronze"],
        plan["watermark"],
        plan["endpoint"],
        plan["access_key"],
        plan["secret_key"],
    )
    write_success_marker()
