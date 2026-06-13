"""Retrieval layer for the Tiki RAG chatbot.

Hybrid search over `rag.product_embeddings` on Postgres+pgvector:
- Optional structured filters (price range, min rating, category) cut the
  candidate set BEFORE vector ANN, so high-selectivity filters don't lose
  recall to the IVFFlat lists parameter.
- Vector cosine similarity then ranks the remaining rows.

This module exposes a single public function `retrieve()` used by app.py.
"""
from __future__ import annotations

import os
from dataclasses import dataclass

import psycopg2
from pgvector.psycopg2 import register_vector

PG_DSN = os.getenv(
    "PG_DSN",
    "postgresql://admin:admin_password@postgres:5432/metastore",
)


@dataclass
class RetrievalFilter:
    min_price: float | None = None
    max_price: float | None = None
    min_rating: float | None = None
    category_keyword: str | None = None  # ILIKE match on category_name


@dataclass
class Hit:
    product_id: int
    product_name: str
    category_name: str | None
    seller_name: str | None
    brand_name: str | None
    price: float | None
    rating_average: float | None
    review_count: int | None
    document: str
    similarity: float


_conn = None


def _get_conn():
    global _conn
    if _conn is None or _conn.closed:
        _conn = psycopg2.connect(PG_DSN)
        register_vector(_conn)
    return _conn


def retrieve(query_embedding: list[float], filt: RetrievalFilter, k: int = 5) -> list[Hit]:
    where = ["TRUE"]
    params: list = []
    if filt.min_price is not None:
        where.append("price >= %s")
        params.append(filt.min_price)
    if filt.max_price is not None:
        where.append("price <= %s")
        params.append(filt.max_price)
    if filt.min_rating is not None:
        where.append("rating_average >= %s")
        params.append(filt.min_rating)
    if filt.category_keyword:
        where.append("category_name ILIKE %s")
        params.append(f"%{filt.category_keyword}%")

    sql = f"""
    SELECT
        product_id, product_name, category_name, seller_name, brand_name,
        price, rating_average, review_count, document,
        1 - (embedding <=> %s::vector) AS similarity
    FROM rag.product_embeddings
    WHERE {' AND '.join(where)}
    ORDER BY embedding <=> %s::vector
    LIMIT %s;
    """
    conn = _get_conn()
    with conn.cursor() as cur:
        cur.execute(sql, [query_embedding, *params, query_embedding, k])
        rows = cur.fetchall()
    return [
        Hit(
            product_id=r[0],
            product_name=r[1],
            category_name=r[2],
            seller_name=r[3],
            brand_name=r[4],
            price=float(r[5]) if r[5] is not None else None,
            rating_average=float(r[6]) if r[6] is not None else None,
            review_count=r[7],
            document=r[8],
            similarity=float(r[9]),
        )
        for r in rows
    ]
