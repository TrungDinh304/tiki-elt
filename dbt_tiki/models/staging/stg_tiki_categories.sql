{{ config(location=external_path('silver')) }}

WITH raw AS (
    SELECT *
    FROM READ_PARQUET(
        's3://{{ var("bronze_bucket") }}/tiki_categories/dt=*/run_id=*/*.parquet',
        hive_partitioning = TRUE,
        union_by_name = TRUE
    )
),

-- Keep the most recent crawl per menu_id so re-runs within the same month
-- don't produce duplicates after we union historical partitions.
ranked AS (
    SELECT
        menu_id,
        category_name,
        link,
        parent_menu_id,
        path,
        dt,
        run_id,
        TRY_CAST(category_id AS BIGINT) AS category_id,
        TRY_CAST(parent_category_id AS BIGINT) AS parent_category_id,
        TRY_CAST(level AS INTEGER) AS category_level,
        TRY_CAST(is_leaf AS BOOLEAN) AS is_leaf,
        STRPTIME(extracted_at, '%Y%m%d_%H%M%S') AS extracted_at_ts,
        ROW_NUMBER() OVER (
            PARTITION BY menu_id
            ORDER BY extracted_at DESC
        ) AS rn
    FROM raw
    WHERE menu_id IS NOT NULL
)

SELECT
    menu_id,
    category_id,
    category_name,
    link,
    parent_menu_id,
    parent_category_id,
    category_level,
    path,
    is_leaf,
    extracted_at_ts,
    dt,
    run_id
FROM ranked
WHERE rn = 1
