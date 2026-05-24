"""Index lakehouse marts into pgvector for the RAG chatbot.

Reads `dim_products` + top-5 reviews per product from `fct_reviews` (both in
the lakehouse bucket), builds a single document per product, embeds it
locally via sentence-transformers (BGE-M3 by default), and upserts into
`rag.product_embeddings` on Postgres.

Idempotent — runs `CREATE EXTENSION` / `CREATE TABLE IF NOT EXISTS` on every
invocation so the table appears the first time without a manual init.
Subsequent runs skip products whose source content hash hasn't changed,
so re-runs after a partial crawl don't waste compute.
"""
from __future__ import annotations

import hashlib
import os
import sys

import duckdb
import psycopg2
from pgvector.psycopg2 import register_vector
from psycopg2.extras import execute_values

# ---------- Config (env-driven, same defaults as analytics_plot.py) ----------
S3_ENDPOINT = os.getenv("S3_ENDPOINT_HOST", "localhost:9000")
S3_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
S3_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minio_password")
LAKEHOUSE_BUCKET = os.getenv("LAKEHOUSE_BUCKET", "lakehouse")
MARTS_PREFIX = f"s3://{LAKEHOUSE_BUCKET}/marts"

PG_DSN = os.getenv(
    "PG_DSN",
    "postgresql://admin:admin_password@postgres:5432/metastore",
)

# Local embedding model. BGE-M3 → 1024 dims. If you switch to a different
# model dimension, drop+recreate rag.product_embeddings (or run migration).
EMBEDDING_MODEL = os.getenv("EMBEDDING_MODEL", "BAAI/bge-m3")
EMBEDDING_DIM = int(os.getenv("EMBEDDING_DIM", "1024"))

BATCH_SIZE = int(os.getenv("RAG_INDEX_BATCH", "32"))
REVIEWS_PER_PRODUCT = int(os.getenv("RAG_REVIEWS_PER_PRODUCT", "5"))


# ---------- DuckDB (read marts from MinIO) ----------
def _duckdb_conn():
    conn = duckdb.connect()
    conn.execute("INSTALL httpfs;")
    conn.execute("LOAD httpfs;")
    conn.execute(f"SET s3_endpoint='{S3_ENDPOINT}';")
    conn.execute(f"SET s3_access_key_id='{S3_ACCESS_KEY}';")
    conn.execute(f"SET s3_secret_access_key='{S3_SECRET_KEY}';")
    conn.execute("SET s3_use_ssl=false;")
    conn.execute("SET s3_url_style='path';")
    return conn


def load_products_with_reviews(duck):
    # One row per product: metadata from dim_products + a newline-joined string
    # of the top-N review titles+contents ranked by thank_count.
    query = f"""
    WITH top_reviews AS (
        SELECT
            product_id,
            COALESCE(title, '') || ' — ' || COALESCE(review_content, '') AS review_text,
            ROW_NUMBER() OVER (
                PARTITION BY product_id
                ORDER BY COALESCE(thank_count, 0) DESC, COALESCE(rating, 0) DESC
            ) AS rn
        FROM read_parquet('{MARTS_PREFIX}/fct_reviews.parquet')
        WHERE review_content IS NOT NULL AND LENGTH(review_content) > 0
    ),
    review_agg AS (
        SELECT
            product_id,
            STRING_AGG(review_text, '\n') AS reviews_concat
        FROM top_reviews
        WHERE rn <= {REVIEWS_PER_PRODUCT}
        GROUP BY product_id
    )
    SELECT
        p.product_id,
        p.product_name,
        p.short_description,
        p.description,
        p.brand_name,
        p.category_id,
        p.category_name,
        p.seller_id,
        p.seller_name,
        p.price,
        p.rating_average,
        p.review_count,
        COALESCE(r.reviews_concat, '') AS reviews_concat
    FROM read_parquet('{MARTS_PREFIX}/dim_products.parquet') AS p
    LEFT JOIN review_agg AS r USING (product_id)
    WHERE p.product_id IS NOT NULL
    """
    return duck.execute(query).fetchall(), [d[0] for d in duck.description]


def build_document(row: dict) -> str:
    parts = [
        f"Sản phẩm: {row['product_name'] or ''}",
        f"Thương hiệu: {row['brand_name'] or ''}",
        f"Danh mục: {row['category_name'] or ''}",
        f"Giá: {row['price'] or ''}",
        f"Đánh giá trung bình: {row['rating_average'] or ''} ({row['review_count'] or 0} reviews)",
        f"Mô tả ngắn: {row['short_description'] or ''}",
        f"Mô tả: {(row['description'] or '')[:1500]}",
    ]
    if row["reviews_concat"]:
        parts.append(f"Đánh giá khách hàng:\n{row['reviews_concat'][:2000]}")
    return "\n".join(p for p in parts if p)


def content_hash(doc: str) -> str:
    return hashlib.sha256(doc.encode("utf-8")).hexdigest()


# ---------- Postgres ----------
def ensure_schema(pg):
    with pg.cursor() as cur:
        cur.execute("CREATE EXTENSION IF NOT EXISTS vector;")
        cur.execute("CREATE SCHEMA IF NOT EXISTS rag;")
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS rag.product_embeddings (
                product_id BIGINT PRIMARY KEY,
                product_name TEXT,
                category_id BIGINT,
                category_name TEXT,
                seller_id BIGINT,
                seller_name TEXT,
                brand_name TEXT,
                price NUMERIC,
                rating_average NUMERIC,
                review_count INT,
                document TEXT,
                content_hash TEXT,
                embedding vector({EMBEDDING_DIM}),
                indexed_at TIMESTAMPTZ DEFAULT NOW()
            );
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS product_embeddings_embedding_idx
            ON rag.product_embeddings
            USING ivfflat (embedding vector_cosine_ops)
            WITH (lists = 100);
            """
        )
    pg.commit()


def fetch_existing_hashes(pg) -> dict[int, str]:
    with pg.cursor() as cur:
        cur.execute("SELECT product_id, content_hash FROM rag.product_embeddings;")
        return {pid: h for pid, h in cur.fetchall()}


def upsert_batch(pg, rows):
    # rows = list of tuples in column order below.
    sql = """
    INSERT INTO rag.product_embeddings (
        product_id, product_name, category_id, category_name,
        seller_id, seller_name, brand_name, price,
        rating_average, review_count, document, content_hash, embedding
    ) VALUES %s
    ON CONFLICT (product_id) DO UPDATE SET
        product_name = EXCLUDED.product_name,
        category_id = EXCLUDED.category_id,
        category_name = EXCLUDED.category_name,
        seller_id = EXCLUDED.seller_id,
        seller_name = EXCLUDED.seller_name,
        brand_name = EXCLUDED.brand_name,
        price = EXCLUDED.price,
        rating_average = EXCLUDED.rating_average,
        review_count = EXCLUDED.review_count,
        document = EXCLUDED.document,
        content_hash = EXCLUDED.content_hash,
        embedding = EXCLUDED.embedding,
        indexed_at = NOW();
    """
    with pg.cursor() as cur:
        execute_values(cur, sql, rows, template=None, page_size=BATCH_SIZE)
    pg.commit()


# ---------- Local embeddings (sentence-transformers) ----------
def embed_batch(model, texts: list[str]) -> list[list[float]]:
    # Normalize so the cosine similarity used by pgvector (<=>) matches the
    # geometry the model was trained on. BGE-M3 expects normalized vectors.
    vecs = model.encode(
        texts,
        batch_size=BATCH_SIZE,
        normalize_embeddings=True,
        show_progress_bar=False,
    )
    return vecs.tolist()


def main():
    from sentence_transformers import SentenceTransformer

    print(f"Loading embedding model: {EMBEDDING_MODEL}")
    model = SentenceTransformer(EMBEDDING_MODEL)

    print(f"DuckDB → MinIO {S3_ENDPOINT} (bucket={LAKEHOUSE_BUCKET})")
    duck = _duckdb_conn()
    try:
        rows, cols = load_products_with_reviews(duck)
    except duckdb.IOException as exc:
        # Marts not materialized yet — fresh `docker compose up` before the
        # daily pipeline ran. Exit 0 so the init service doesn't fail the
        # whole stack; rerun after the pipeline produces dim_products/fct_reviews.
        msg = str(exc)
        if "No files found" in msg or "does not exist" in msg or "HTTP 404" in msg:
            print(
                "Marts not yet materialized in lakehouse bucket — skipping. "
                "Re-run after the daily pipeline produces "
                "dim_products.parquet + fct_reviews.parquet."
            )
            return 0
        raise
    finally:
        duck.close()
    if not rows:
        print("No products found in dim_products — nothing to index.")
        return 0
    print(f"Loaded {len(rows)} products from marts")

    pg = psycopg2.connect(PG_DSN)
    ensure_schema(pg)
    # Must register the vector adapter AFTER `CREATE EXTENSION vector` runs,
    # otherwise psycopg2 can't look up the vector OID.
    register_vector(pg)
    existing = fetch_existing_hashes(pg)

    # Build documents + filter out unchanged products.
    pending = []
    for r in rows:
        row = dict(zip(cols, r))
        doc = build_document(row)
        h = content_hash(doc)
        if existing.get(row["product_id"]) == h:
            continue
        pending.append((row, doc, h))

    print(f"{len(pending)} products need (re-)embedding ({len(rows) - len(pending)} unchanged)")
    if not pending:
        pg.close()
        return 0

    indexed = 0
    for i in range(0, len(pending), BATCH_SIZE):
        chunk = pending[i : i + BATCH_SIZE]
        texts = [doc for _, doc, _ in chunk]
        vectors = embed_batch(model, texts)
        batch_rows = []
        for (row, doc, h), vec in zip(chunk, vectors):
            batch_rows.append(
                (
                    row["product_id"],
                    row["product_name"],
                    row["category_id"],
                    row["category_name"],
                    row["seller_id"],
                    row["seller_name"],
                    row["brand_name"],
                    row["price"],
                    row["rating_average"],
                    row["review_count"],
                    doc,
                    h,
                    vec,
                )
            )
        upsert_batch(pg, batch_rows)
        indexed += len(chunk)
        print(f"  upserted {indexed}/{len(pending)}")

    pg.close()
    print(f"Done — indexed {indexed} products into rag.product_embeddings.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
