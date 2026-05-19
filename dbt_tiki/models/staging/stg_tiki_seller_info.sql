WITH raw AS (
    SELECT *
    FROM READ_PARQUET(
        's3://{{ var("bronze_bucket") }}/tiki_sellers/dt=*/run_id=*/*.parquet',
        hive_partitioning = TRUE,
        union_by_name = TRUE
    )
),

flattened AS (
    SELECT
        dt,
        run_id,
        TRY_CAST(JSON_EXTRACT_STRING(data, '$.seller.id') AS BIGINT) AS seller_id,
        JSON_EXTRACT_STRING(data, '$.seller.name') AS seller_name,
        JSON_EXTRACT_STRING(data, '$.seller.slug') AS seller_slug,
        JSON_EXTRACT_STRING(data, '$.seller.logo') AS seller_logo,
        JSON_EXTRACT_STRING(data, '$.seller.icon') AS seller_icon,
        JSON_EXTRACT_STRING(data, '$.seller.store_level') AS store_level,
        TRY_CAST(JSON_EXTRACT_STRING(data, '$.seller.review_count') AS INTEGER)
            AS seller_review_count,
        TRY_CAST(JSON_EXTRACT_STRING(data, '$.seller.avg_rating_point') AS FLOAT)
            AS avg_rating_point,
        TRY_CAST(JSON_EXTRACT_STRING(data, '$.seller.total_follower') AS INTEGER) AS total_follower,
        TRY_CAST(JSON_EXTRACT_STRING(data, '$.seller.is_official') AS BOOLEAN) AS is_official,
        JSON_EXTRACT_STRING(data, '$.seller.url_key') AS url_key,
        STRPTIME(extracted_at, '%Y%m%d_%H%M%S') AS extracted_at_ts
    FROM raw
    WHERE data IS NOT NULL
),

ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY seller_id
            ORDER BY extracted_at_ts DESC
        ) AS rn
    FROM flattened
    WHERE seller_id IS NOT NULL
)

SELECT
    seller_id,
    seller_name,
    seller_slug,
    seller_logo,
    seller_icon,
    store_level,
    seller_review_count,
    avg_rating_point,
    total_follower,
    is_official,
    url_key,
    extracted_at_ts,
    dt,
    run_id
FROM ranked
WHERE rn = 1
