{{ config(materialized='table') }}

WITH distinct_assets AS (
    SELECT DISTINCT symbol, asset_type, source
    FROM {{ ref('stg_prices') }}
),
with_type_id AS (
    SELECT
        d.symbol,
        d.source,
        t.asset_type_id,
        d.asset_type
    FROM distinct_assets d
    JOIN {{ ref('dim_asset_type') }} t ON t.name = d.asset_type
)
SELECT
    ROW_NUMBER() OVER (ORDER BY w.symbol) AS asset_id,
    w.symbol,
    COALESCE(c.name, w.symbol) AS name,
    w.asset_type_id,
    w.asset_type,
    w.source,
    COALESCE(c.price_completeness,
             CASE WHEN w.source = 'coingecko' THEN 'close_volume_only' ELSE 'ohlcv' END
    ) AS price_completeness,
    COALESCE(c.base_ccy, 'USD') AS base_ccy
FROM with_type_id w
LEFT JOIN {{ ref('asset_catalog') }} c ON c.symbol = w.symbol
