WITH reviews AS (
    SELECT * FROM {{ ref('stg_tiki_reviews') }}
),

products AS (
    SELECT
        product_id,
        product_name,
        brand_name
    FROM {{ ref('stg_tiki_books') }}
),

sellers AS (
    SELECT
        seller_id,
        seller_name
    FROM {{ ref('stg_tiki_seller_info') }}
)

SELECT
    r.review_id,
    r.product_id,
    p.product_name,
    p.brand_name,
    r.seller_id,
    s.seller_name,
    r.rating,
    r.score,
    r.title,
    r.review_content,
    r.thank_count,
    r.customer_id,
    r.customer_name,
    r.created_at,
    r.purchased_at,
    r.extracted_at_ts,
    r.dt
FROM reviews AS r
LEFT JOIN products AS p ON r.product_id = p.product_id
LEFT JOIN sellers AS s ON r.seller_id = s.seller_id
