{{ config(location=external_path('lakehouse_marts')) }}

WITH products AS (
    SELECT * FROM {{ ref('stg_tiki_products') }}
),

sellers AS (
    SELECT
        seller_id,
        seller_name,
        is_official,
        store_level
    FROM {{ ref('stg_tiki_seller_info') }}
),

-- The listings API doesn't surface category info, so we pull category_id
-- from the product-detail staging to wire the snowflake FK into dim_categories.
product_category AS (
    SELECT
        product_id,
        category_id
    FROM {{ ref('stg_tiki_product_details') }}
)

SELECT
    p.product_id,
    p.product_name,
    p.author_name,
    p.brand_name,
    p.price,
    p.original_price,
    p.discount,
    p.discount_rate,
    p.rating_average,
    p.review_count,
    p.inventory_status,
    p.quantity_sold,
    p.seller_id,
    s.seller_name,
    s.is_official,
    s.store_level,
    pc.category_id,
    p.thumbnail_url,
    p.extracted_at_ts,
    p.dt
FROM products AS p
LEFT JOIN sellers AS s ON p.seller_id = s.seller_id
LEFT JOIN product_category AS pc ON p.product_id = pc.product_id
