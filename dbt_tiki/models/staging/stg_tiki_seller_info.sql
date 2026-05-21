{{ config(location=external_path('silver')) }}

-- Strict, narrow read to avoid two distinct DuckDB binder failures we hit on
-- this prefix:
--
--   1. `union_by_name = TRUE` with stray top-level keys from Tiki's seller
--      response (ad/meta/...) gave a `BIGINT != VARCHAR` INTERNAL assertion
--      because pandas inferred different parquet types per batch. The crawler
--      now keeps only the `data` field, fixing this going forward.
--
--   2. `hive_partitioning = TRUE` plus `df["dt"] = RUN_DT` in the crawler
--      double-sources the `dt`/`run_id` columns: one VARCHAR from the parquet
--      payload, one DATE/VARCHAR from the hive path. union_by_name then tries
--      to merge them and fails with `VARCHAR != DATE`. We disable hive
--      partitioning here — dt/run_id are already in the data columns, the
--      path globs work either way.
WITH raw AS (
    SELECT
        CAST(data AS VARCHAR) AS data,
        CAST(extracted_at AS VARCHAR) AS extracted_at,
        CAST(dt AS VARCHAR) AS dt,
        CAST(run_id AS VARCHAR) AS run_id
    FROM READ_PARQUET(
        's3://{{ var("bronze_bucket") }}/tiki_sellers/dt=*/run_id=*/*.parquet',
        hive_partitioning = FALSE,
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
)

-- DISTINCT ON instead of the usual `ROW_NUMBER() OVER ... WHERE rn = 1`
-- pattern. DuckDB 1.10.1 has an INTERNAL binder bug ("[32.0] BIGINT !=
-- VARCHAR") when the ranked+rn pattern runs under CREATE TABLE AS — which is
-- exactly what dbt-duckdb's `external` materialization does as its
-- intermediate step. The same query passes as a plain SELECT or as a COPY,
-- so the data and the rest of the pipeline are fine; only this CTAS shape
-- trips the binder. DISTINCT ON dedupes via a different operator and dodges
-- the bug. Pick the latest row per seller_id by ordering DESC on
-- extracted_at_ts.
SELECT DISTINCT ON (seller_id)
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
FROM flattened
WHERE seller_id IS NOT NULL
ORDER BY seller_id, extracted_at_ts DESC
