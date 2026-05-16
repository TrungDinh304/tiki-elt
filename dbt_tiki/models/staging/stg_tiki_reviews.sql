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
        TRY_CAST(product_id AS BIGINT) AS product_id,
        TRY_CAST(seller_id AS BIGINT)  AS seller_id,
        TRY_CAST(page AS INTEGER)      AS page,
        UNNEST(
            CAST(
                json_extract(data, '$[*]') AS JSON[]
            )
        ) AS review_json,
        STRPTIME(extracted_at, '%Y%m%d_%H%M%S') AS extracted_at_ts,
        dt,
        run_id
    FROM raw
    WHERE data IS NOT NULL AND data <> '[]'
),

flattened AS (
    SELECT
        TRY_CAST(json_extract_string(review_json, '$.id') AS BIGINT)              AS review_id,
        product_id,
        seller_id,
        page,
        TRY_CAST(json_extract_string(review_json, '$.rating') AS INTEGER)         AS rating,
        json_extract_string(review_json, '$.title')                               AS title,
        json_extract_string(review_json, '$.content')                             AS content,
        TRY_CAST(json_extract_string(review_json, '$.thank_count') AS INTEGER)    AS thank_count,
        TRY_CAST(json_extract_string(review_json, '$.score') AS FLOAT)            AS score,
        TRY_CAST(json_extract_string(review_json, '$.customer_id') AS BIGINT)     AS customer_id,
        json_extract_string(review_json, '$.created_by.full_name')                AS customer_name,
        json_extract_string(review_json, '$.purchased_at')                        AS purchased_at,
        TRY_CAST(json_extract_string(review_json, '$.created_at') AS BIGINT)      AS created_at_unix,
        extracted_at_ts,
        dt,
        run_id
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
    content,
    thank_count,
    score,
    customer_id,
    customer_name,
    purchased_at,
    TO_TIMESTAMP(created_at_unix) AS created_at,
    extracted_at_ts,
    dt,
    run_id
FROM ranked
WHERE rn = 1
