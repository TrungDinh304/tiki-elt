{{ config(location=external_path('lakehouse_marts')) }}

SELECT
    seller_id,
    seller_name,
    seller_slug,
    seller_logo,
    store_level,
    seller_review_count,
    avg_rating_point,
    total_follower,
    is_official,
    url_key,
    extracted_at_ts AS last_seen_at,
    dt
FROM {{ ref('stg_tiki_seller_info') }}
