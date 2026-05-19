"""Crawl Tiki's top-level category tree from the menu-config API.

Writes a flattened snapshot to the bronze layer using the same Hive-style
partitioning as fetch_tiki.py:
    s3://bronze/tiki_categories/dt=YYYY-MM-DD/run_id=YYYYMMDD_HHMMSS/categories.parquet

Categories change slowly, so this crawl is scheduled monthly by Airflow.
"""

import os
import re
import json
import time
import random
import boto3
import requests
import pandas as pd
from datetime import datetime
from io import BytesIO
from dotenv import load_dotenv

load_dotenv()

CATEGORY_URL = "https://api.tiki.vn/raiden/v2/menu-config?platform=desktop"

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minio_password")
BRONZE_BUCKET = os.getenv("BRONZE_BUCKET", "bronze")
ENTITY = "tiki_categories"

_RUN_TS = datetime.now()
RUN_ID = _RUN_TS.strftime("%Y%m%d_%H%M%S")
RUN_DT = _RUN_TS.strftime("%Y-%m-%d")

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"})

# Tiki category links end with `/c<id>` — that numeric id is the canonical
# category_id used by product detail responses, so we extract it for joins.
_CATEGORY_ID_RE = re.compile(r"/c(\d+)(?:[/?#]|$)")


def fetch_menu(max_retries=3):
    for attempt in range(1, max_retries + 1):
        try:
            resp = SESSION.get(CATEGORY_URL, timeout=15)
            if resp.status_code == 200:
                return resp.json()
            print(f"menu-config returned {resp.status_code}")
        except requests.RequestException as err:
            print(f"menu-config request failed: {err}")

        if attempt < max_retries:
            delay = 1.5 * attempt + random.uniform(0, 0.5)
            print(f"Retrying in {delay:.1f}s...")
            time.sleep(delay)

    print("Giving up on menu-config")
    return None


def _extract_category_id(link):
    if not link or not isinstance(link, str):
        return None
    match = _CATEGORY_ID_RE.search(link)
    return int(match.group(1)) if match else None


def _iter_menu_roots(payload):
    """Yield top-level menu item lists from the menu-config payload.

    The API has historically nested its tree under different keys
    (`menu_block.items`, `menu`, `items`, `data`), so we probe each and fall
    back to scanning any list-valued field that looks like menu nodes.
    """
    if not isinstance(payload, dict):
        return

    candidates = [
        (
            payload.get("menu_block", {}).get("items")
            if isinstance(payload.get("menu_block"), dict)
            else None
        ),
        payload.get("menu"),
        payload.get("items"),
        payload.get("data"),
    ]
    for c in candidates:
        if isinstance(c, list) and c:
            yield c
            return

    for value in payload.values():
        if isinstance(value, list) and value and isinstance(value[0], dict):
            yield value
            return


def _first_present(node, keys):
    for k in keys:
        v = node.get(k)
        if v:
            return v
    return None


def _node_children(node):
    children = _first_present(node, ("children", "sub_menu", "items")) or []
    return children if isinstance(children, list) else []


def _build_row(
    node,
    menu_id,
    name,
    link,
    category_id,
    parent_menu_id,
    parent_category_id,
    level,
    current_path,
    is_leaf,
):
    return {
        "menu_id": str(menu_id) if menu_id is not None else None,
        "category_id": category_id,
        "category_name": name,
        "link": link,
        "parent_menu_id": str(parent_menu_id) if parent_menu_id is not None else None,
        "parent_category_id": parent_category_id,
        "level": level,
        "path": " > ".join(current_path) if current_path else None,
        "is_leaf": is_leaf,
        "raw_node": node,
    }


def _visit(node, parent_menu_id, parent_category_id, level, path_parts, rows):
    if not isinstance(node, dict):
        return

    menu_id = _first_present(node, ("id", "code", "key"))
    name = _first_present(node, ("text", "name", "title", "label"))
    link = _first_present(node, ("link", "url", "href"))
    category_id = _extract_category_id(link)
    children = _node_children(node)

    current_path = path_parts + ([name] if name else [])
    is_leaf = len(children) == 0

    if menu_id is not None or category_id is not None or name:
        rows.append(
            _build_row(
                node,
                menu_id,
                name,
                link,
                category_id,
                parent_menu_id,
                parent_category_id,
                level,
                current_path,
                is_leaf,
            )
        )

    for child in children:
        _visit(child, menu_id, category_id, level + 1, current_path, rows)


def flatten_categories(payload):
    """Walk the (possibly nested) menu tree into flat (id, parent, level, path) rows."""
    rows = []
    for roots in _iter_menu_roots(payload):
        for root in roots:
            _visit(root, None, None, 1, [], rows)
    return rows


def _json_default(value):
    if isinstance(value, (set, tuple)):
        return list(value)
    return str(value)


def sanitize_dataframe(df):
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


def _s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )


def save_to_minio(rows):
    if not rows:
        print("No category rows to save")
        return 0

    df = pd.DataFrame(rows)
    df["extracted_at"] = RUN_ID
    df["dt"] = RUN_DT
    df["run_id"] = RUN_ID
    df = sanitize_dataframe(df)

    buf = BytesIO()
    df.to_parquet(buf, index=False)

    key = f"{ENTITY}/dt={RUN_DT}/run_id={RUN_ID}/categories.parquet"
    print(f"Uploading {len(df)} category rows to {BRONZE_BUCKET}/{key}")
    _s3_client().put_object(Bucket=BRONZE_BUCKET, Key=key, Body=buf.getvalue())
    print("Upload complete.")
    return len(df)


def write_success_marker(row_count):
    body = json.dumps(
        {
            "dt": RUN_DT,
            "run_id": RUN_ID,
            "entity": ENTITY,
            "row_count": row_count,
            "completed_at": datetime.now().isoformat(),
        }
    )
    key = f"_manifests/{ENTITY}/dt={RUN_DT}/{RUN_ID}.json"
    _s3_client().put_object(Bucket=BRONZE_BUCKET, Key=key, Body=body)
    print(f"Manifest written: {BRONZE_BUCKET}/{key}")


def main():
    payload = fetch_menu()
    if not payload:
        raise SystemExit("Failed to fetch Tiki menu-config")

    rows = flatten_categories(payload)
    if not rows:
        # Persist the raw payload for debugging so we can adjust the parser
        # without re-crawling — menu-config shape has changed in the past.
        debug_key = f"{ENTITY}/_debug/dt={RUN_DT}/run_id={RUN_ID}/raw.json"
        _s3_client().put_object(
            Bucket=BRONZE_BUCKET,
            Key=debug_key,
            Body=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        )
        raise SystemExit(f"Parsed 0 categories; raw payload dumped to {BRONZE_BUCKET}/{debug_key}")

    count = save_to_minio(rows)
    write_success_marker(count)


if __name__ == "__main__":
    main()
