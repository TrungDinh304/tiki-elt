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
        TRY_CAST(json_extract_string(data, '$.seller.id') AS BIGINT)            AS seller_id,
        json_extract_string(data, '$.seller.name')                              AS seller_name,
        json_extract_string(data, '$.seller.slug')                              AS seller_slug,
        json_extract_string(data, '$.seller.logo')                              AS seller_logo,
        json_extract_string(data, '$.seller.icon')                              AS seller_icon,
        json_extract_string(data, '$.seller.store_level')                       AS store_level,
        TRY_CAST(json_extract_string(data, '$.seller.review_count') AS INTEGER) AS seller_review_count,
        TRY_CAST(json_extract_string(data, '$.seller.avg_rating_point') AS FLOAT) AS avg_rating_point,
        TRY_CAST(json_extract_string(data, '$.seller.total_follower') AS INTEGER) AS total_follower,
        TRY_CAST(json_extract_string(data, '$.seller.is_official') AS BOOLEAN)  AS is_official,
        json_extract_string(data, '$.seller.url_key')                           AS url_key,
        STRPTIME(extracted_at, '%Y%m%d_%H%M%S')                                 AS extracted_at_ts,
        dt,
        run_id
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
