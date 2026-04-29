{{ config(materialized='view') }}

-- Stable ref() handle over Python-maintained silver.stg_prices.
-- A view rather than a table — dbt doesn't own the materialization, just the reference.

SELECT
    source,
    asset_type,
    symbol,
    date,
    open,
    high,
    low,
    close,
    volume,
    vwap,
    trade_count,
    ingested_at,
    run_id
FROM {{ source('silver', 'stg_prices') }}
