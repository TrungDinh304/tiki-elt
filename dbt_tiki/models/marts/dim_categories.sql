{{ config(location=external_path('lakehouse_marts')) }}

-- Snowflake-style category dimension: a single self-referencing table with
-- parent_category_id pointing back to dim_categories.category_id. Rows
-- without an extractable category_id (link didn't match /c<digits>) are
-- dropped because they can't be joined to products.
WITH staged AS (
    SELECT *
    FROM {{ ref('stg_tiki_categories') }}
    WHERE category_id IS NOT NULL
),

-- A menu may surface the same category_id under multiple parents; keep the
-- shallowest occurrence as the canonical row so the dim has one row per
-- category_id (a hard requirement for a clean PK on the product join).
ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY category_id
            ORDER BY category_level ASC, extracted_at_ts DESC
        ) AS rn
    FROM staged
)

SELECT
    category_id,
    category_name,
    parent_category_id,
    category_level,
    path,
    is_leaf,
    link,
    extracted_at_ts AS last_seen_at,
    dt
FROM ranked
WHERE rn = 1
