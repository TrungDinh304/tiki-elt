WITH raw AS (
    SELECT *
    FROM READ_PARQUET(
        's3://{{ var("bronze_bucket") }}/tiki_reviews/dt=*/run_id=*/*.parquet',
        hive_partitioning = TRUE,
        union_by_name = TRUE
    )
),

exploded AS (
    SELECT
        dt,
        run_id,
        TRY_CAST(product_id AS BIGINT) AS product_id,
        TRY_CAST(seller_id AS BIGINT) AS seller_id,
        TRY_CAST(page AS INTEGER) AS page,
        UNNEST(
            CAST(
                JSON_EXTRACT(data, '$[*]') AS JSON []
            )
        ) AS review_json,
        STRPTIME(extracted_at, '%Y%m%d_%H%M%S') AS extracted_at_ts
    FROM raw
    WHERE data IS NOT NULL AND data <> '[]'
),

flattened AS (
    SELECT
        product_id,
        seller_id,
        page,
        extracted_at_ts,
        dt,
        run_id,
        TRY_CAST(JSON_EXTRACT_STRING(review_json, '$.id') AS BIGINT) AS review_id,
        TRY_CAST(JSON_EXTRACT_STRING(review_json, '$.rating') AS INTEGER) AS rating,
        JSON_EXTRACT_STRING(review_json, '$.title') AS title,
        JSON_EXTRACT_STRING(review_json, '$.content') AS review_content,
        TRY_CAST(JSON_EXTRACT_STRING(review_json, '$.thank_count') AS INTEGER) AS thank_count,
        TRY_CAST(JSON_EXTRACT_STRING(review_json, '$.score') AS FLOAT) AS score,
        TRY_CAST(JSON_EXTRACT_STRING(review_json, '$.customer_id') AS BIGINT) AS customer_id,
        JSON_EXTRACT_STRING(review_json, '$.created_by.full_name') AS customer_name,
        JSON_EXTRACT_STRING(review_json, '$.purchased_at') AS purchased_at,
        TRY_CAST(JSON_EXTRACT_STRING(review_json, '$.created_at') AS BIGINT) AS created_at_unix
    FROM exploded
),

ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY review_id
            ORDER BY extracted_at_ts DESC
        ) AS rn
    FROM flattened
    WHERE review_id IS NOT NULL
)

SELECT
    review_id,
    product_id,
    seller_id,
    page,
    rating,
    title,
    review_content,
    thank_count,
    score,
    customer_id,
    customer_name,
    purchased_at,
    extracted_at_ts,
    dt,
    run_id,
    TO_TIMESTAMP(created_at_unix) AS created_at
FROM ranked
WHERE rn = 1
