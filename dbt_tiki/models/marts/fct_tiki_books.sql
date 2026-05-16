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
    b.thumbnail_url,
    b.extracted_at_ts,
    b.dt
FROM books b
LEFT JOIN sellers s USING (seller_id)
