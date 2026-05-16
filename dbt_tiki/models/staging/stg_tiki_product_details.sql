WITH raw AS (
    SELECT *
    FROM READ_PARQUET(
        's3://{{ var("bronze_bucket") }}/tiki_product_details/dt=*/run_id=*/*.parquet',
        hive_partitioning = TRUE,
        union_by_name = TRUE
    )
),

ranked AS (
    SELECT
        TRY_CAST(id AS BIGINT)                                        AS product_id,
        master_id,
        sku,
        name                                                          AS product_name,
        short_description,
        description,
        TRY_CAST(price AS INTEGER)                                    AS price,
        TRY_CAST(list_price AS INTEGER)                               AS list_price,
        TRY_CAST(original_price AS INTEGER)                           AS original_price,
        TRY_CAST(discount AS INTEGER)                                 AS discount,
        TRY_CAST(discount_rate AS FLOAT)                              AS discount_rate,
        TRY_CAST(rating_average AS FLOAT)                             AS rating_average,
        TRY_CAST(review_count AS INTEGER)                             AS review_count,
        TRY_CAST(all_time_quantity_sold AS INTEGER)                   AS all_time_quantity_sold,
        TRY_CAST(json_extract_string(quantity_sold, '$.value') AS INTEGER) AS quantity_sold,
        TRY_CAST(json_extract_string(current_seller, '$.id') AS BIGINT)     AS seller_id,
        json_extract_string(current_seller, '$.name')                       AS seller_name,
        json_extract_string(brand, '$.name')                                AS brand_name,
        TRY_CAST(json_extract_string(brand, '$.id') AS BIGINT)              AS brand_id,
        type,
        inventory_status,
        url_key,
        url_path,
        categories,
        breadcrumbs,
        specifications,
        images,
        STRPTIME(extracted_at, '%Y%m%d_%H%M%S')                       AS extracted_at_ts,
        dt,
        run_id,
        ROW_NUMBER() OVER (
            PARTITION BY id
            ORDER BY extracted_at DESC
        ) AS rn
    FROM raw
    WHERE id IS NOT NULL
)

SELECT
    product_id,
    master_id,
    sku,
    product_name,
    short_description,
    description,
    price,
    list_price,
    original_price,
    discount,
    discount_rate,
    rating_average,
    review_count,
    all_time_quantity_sold,
    quantity_sold,
    seller_id,
    seller_name,
    brand_id,
    brand_name,
    type,
    inventory_status,
    url_key,
    url_path,
    categories,
    breadcrumbs,
    specifications,
    images,
    extracted_at_ts,
    dt,
    run_id
FROM ranked
WHERE rn = 1
