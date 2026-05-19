WITH books AS (
    SELECT * FROM {{ ref('stg_tiki_books') }}
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
    b.product_id,
    b.product_name,
    b.author_name,
    b.brand_name,
    b.price,
    b.original_price,
    b.discount,
    b.discount_rate,
    b.rating_average,
    b.review_count,
    b.inventory_status,
    b.quantity_sold,
    b.seller_id,
    s.seller_name,
    s.is_official,
    s.store_level,
    pc.category_id,
    b.thumbnail_url,
    b.extracted_at_ts,
    b.dt
FROM books b
LEFT JOIN sellers s USING (seller_id)
LEFT JOIN product_category pc USING (product_id)
