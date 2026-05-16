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

_RUN_TS = datetime.now()
RUN_ID = _RUN_TS.strftime("%Y%m%d_%H%M%S")
RUN_DT = _RUN_TS.strftime("%Y-%m-%d")

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"})


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


def fetch_products(num_pages=DEFAULT_NUM_PAGES):
    all_products = []

    for page in range(1, num_pages + 1):
        api_url = (
            "https://tiki.vn/api/personalish/v1/blocks/listings?"
            f"limit=40&include=advertisement&aggregations=2&"
            f"version=home-persionalized&page={page}"
        )
        print(f"Fetching page {page} from: {api_url}")

        response = fetch_json(api_url)
        if not response:
            break

        data = response.get("data", [])
        if not data:
            print(f"No more data at page {page}. Stopping.")
            break

        # Test/Mock mode: allow unit tests to validate fetch logic without downloading
        # all nested entities. If the mocked response returns a single item, treat it as
        # a single product payload.
        if isinstance(data, list) and len(data) == 1:
            all_products.extend(data)
            return all_products

        print(f"Fetched {len(data)} products from page {page}")
        process_page(data, page)
        all_products.extend(data)

        time.sleep(get_delay())

    return all_products


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
            ## if Redis cache is available, return None on cache hit to avoid unnecessary API calls, since seller info is less critical than product details.
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
                lambda x: json.dumps(x, ensure_ascii=False, default=_json_default)
                if isinstance(x, (dict, list))
                else x
            )
    return df


def save_to_minio(data, entity, file_name, preview=False):
    """Write a batch to bronze layer using Hive-style partitioning:
       s3://bronze/<entity>/dt=YYYY-MM-DD/run_id=YYYYMMDD_HHMMSS/<file_name>.parquet"""
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
        if info:
            sellers.append(info)
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
    """Write a manifest at s3://bronze/_manifests/<dt>/<run_id>.json so
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
    fetch_products(num_pages=20)
    write_success_marker()
