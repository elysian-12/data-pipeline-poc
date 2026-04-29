-- Singular test: for stock assets, the number of *trading-day* gaps should be zero.
-- A gap is a trading day (dim_date.is_trading_day=true) with no fact row for that asset.
-- Returns rows on failure.

WITH stock_assets AS (
    SELECT asset_id, symbol FROM {{ ref('dim_asset') }} WHERE asset_type = 'stock'
),
asset_dates AS (
    SELECT
        sa.asset_id,
        sa.symbol,
        d.date,
        d.date_id
    FROM stock_assets sa
    CROSS JOIN {{ ref('dim_date') }} d
    WHERE d.is_trading_day = TRUE
),
coverage AS (
    SELECT
        ad.asset_id,
        ad.symbol,
        ad.date,
        CASE WHEN fp.close IS NULL THEN 1 ELSE 0 END AS is_gap
    FROM asset_dates ad
    LEFT JOIN {{ ref('fact_daily_price') }} fp
      ON fp.asset_id = ad.asset_id AND fp.date_id = ad.date_id
)
SELECT asset_id, symbol, date
FROM coverage
WHERE is_gap = 1
