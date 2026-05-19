WITH raw AS (
    SELECT *
    FROM READ_PARQUET(
        's3://{{ var("bronze_bucket") }}/tiki_products/dt=*/run_id=*/*.parquet',
        hive_partitioning = TRUE,
        union_by_name = TRUE
    )
),

ranked AS (
    SELECT
        name AS product_name,
        author_name,
        brand_name,
        inventory_status,
        thumbnail_url,
        dt,
        run_id,
        TRY_CAST(id AS BIGINT) AS product_id,
        TRY_CAST(price AS INTEGER) AS price,
        TRY_CAST(original_price AS INTEGER) AS original_price,
        TRY_CAST(discount AS INTEGER) AS discount,
        TRY_CAST(discount_rate AS FLOAT) AS discount_rate,
        TRY_CAST(rating_average AS FLOAT) AS rating_average,
        TRY_CAST(review_count AS INTEGER) AS review_count,
        TRY_CAST(
            JSON_EXTRACT_STRING(quantity_sold, '$.value') AS INTEGER
        ) AS quantity_sold,
        TRY_CAST(seller_id AS BIGINT) AS seller_id,
        STRPTIME(extracted_at, '%Y%m%d_%H%M%S') AS extracted_at_ts,
        ROW_NUMBER() OVER (
            PARTITION BY id
            ORDER BY extracted_at DESC
        ) AS rn
    FROM raw
    WHERE id IS NOT NULL
)

SELECT
    product_id,
    product_name,
    author_name,
    brand_name,
    inventory_status,
    price,
    original_price,
    discount,
    discount_rate,
    rating_average,
    review_count,
    quantity_sold,
    seller_id,
    thumbnail_url,
    extracted_at_ts,
    dt,
    run_id
FROM ranked
WHERE rn = 1
