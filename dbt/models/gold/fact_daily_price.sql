{{
  config(
    materialized='incremental',
    unique_key=['asset_id', 'date_id'],
    on_schema_change='append_new_columns'
  )
}}

-- Grain: one row per (asset_id, date_id). OHL nullable for close+volume-only sources.

WITH priced AS (
    SELECT
        p.symbol,
        p.date,
        p.open,
        p.high,
        p.low,
        p.close,
        p.volume,
        p.vwap,
        p.trade_count,
        p.ingested_at
    FROM {{ ref('stg_prices') }} p
    {% if is_incremental() %}
    -- Only consider silver rows that are newer than anything already in the fact.
    WHERE p.ingested_at > (SELECT COALESCE(MAX(ingested_at), TIMESTAMP '1970-01-01') FROM {{ this }})
    {% endif %}
),
final AS (
    SELECT
        a.asset_id,
        d.date_id,
        pr.open,
        pr.high,
        pr.low,
        pr.close,
        pr.volume,
        pr.vwap,
        pr.trade_count,
        pr.ingested_at
    FROM priced pr
    JOIN {{ ref('dim_asset') }} a ON a.symbol = pr.symbol
    JOIN {{ ref('dim_date') }}  d ON d.date  = pr.date
)
SELECT * FROM final
