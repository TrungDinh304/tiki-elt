{{ config(location=external_path('silver')) }}

{#- Đọc cả live partition lẫn _processed/ — archive_processed.py move
    bronze/tiki_categories/dt=*/ sang bronze/tiki_categories/_processed/dt=*/
    ngay sau khi monthly DAG dbt xong. Nếu chỉ glob live path, các daily DAG
    rebuild dbt sau đó sẽ thấy 0 file → dim_categories ghi đè bằng empty.
    Categories là small data (~14KB/run), re-read cumulative không tốn gì.

    DuckDB READ_PARQUET với list of patterns sẽ FAIL nếu bất kỳ pattern nào
    không match (không skip silently), nên phải build list động — chỉ thêm
    path có file. -#}
{%- set bb = var("bronze_bucket") -%}
{%- set live_pat = "'s3://" ~ bb ~ "/tiki_categories/dt=*/run_id=*/*.parquet'" -%}
{%- set arch_pat = "'s3://" ~ bb ~ "/tiki_categories/_processed/dt=*/run_id=*/*.parquet'" -%}
{%- set paths = [] -%}
{%- if execute -%}
    {%- set live_n = run_query("SELECT COUNT(*) FROM glob(" ~ live_pat ~ ")")[0][0] -%}
    {%- if live_n > 0 %}{% do paths.append(live_pat) %}{% endif -%}
    {%- set arch_n = run_query("SELECT COUNT(*) FROM glob(" ~ arch_pat ~ ")")[0][0] -%}
    {%- if arch_n > 0 %}{% do paths.append(arch_pat) %}{% endif -%}
{%- endif %}

{% if paths|length > 0 %}
WITH raw AS (
    SELECT *
    FROM READ_PARQUET(
        [{{ paths|join(', ') }}],
        hive_partitioning = TRUE,
        union_by_name = TRUE
    )
),

-- Dedup theo category_id (extract từ link `/c<digits>`). Trước đây dùng
-- menu_id, nhưng Tiki menu-config API gần đây không trả id/code/key trên
-- node nào → menu_id 100% NULL → filter giết sạch dataset. category_id còn
-- nguyên 100% rows và cũng là khoá join về dim_products, nên là khoá dedup
-- chính xác hơn.
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
            PARTITION BY TRY_CAST(category_id AS BIGINT)
            ORDER BY extracted_at DESC
        ) AS rn
    FROM raw
    WHERE category_id IS NOT NULL
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

{% else %}
-- No new category files found - return empty result set with correct schema
SELECT
    NULL::VARCHAR as menu_id,
    NULL::BIGINT as category_id,
    NULL::VARCHAR as category_name,
    NULL::VARCHAR as link,
    NULL::VARCHAR as parent_menu_id,
    NULL::BIGINT as parent_category_id,
    NULL::INTEGER as category_level,
    NULL::VARCHAR as path,
    NULL::BOOLEAN as is_leaf,
    NULL::TIMESTAMP as extracted_at_ts,
    NULL::VARCHAR as dt,
    NULL::VARCHAR as run_id
WHERE FALSE
{% endif %}
