{{ config(materialized='table') }}

-- Full rebuild each run. Window functions over the entire price history mean
-- mid-history backfills would corrupt incremental state; at 3k rows the rebuild
-- is sub-second so the correctness trade-off is free.

WITH with_returns AS (
    SELECT
        f.asset_id,
        f.date_id,
        d.date,
        f.close,
        (f.close / NULLIF(LAG(f.close) OVER (PARTITION BY f.asset_id ORDER BY d.date), 0)) - 1.0
            AS daily_return,
        LN(f.close / NULLIF(LAG(f.close) OVER (PARTITION BY f.asset_id ORDER BY d.date), 0))
            AS log_return
    FROM {{ ref('fact_daily_price') }} f
    JOIN {{ ref('dim_date') }} d ON d.date_id = f.date_id
),
rolling AS (
    SELECT
        *,
        {{ rolling_return('close', 'date', 'asset_id', 7)   }} AS rolling_return_7d,
        {{ rolling_return('close', 'date', 'asset_id', 30)  }} AS rolling_return_30d,
        {{ rolling_return('close', 'date', 'asset_id', 90)  }} AS rolling_return_90d,
        {{ rolling_return('close', 'date', 'asset_id', 180) }} AS rolling_return_180d,
        {{ rolling_return('close', 'date', 'asset_id', 365) }} AS rolling_return_365d,
        CASE
            WHEN COUNT(daily_return) OVER (
                PARTITION BY asset_id ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
            ) >= 30
            THEN STDDEV(daily_return) OVER (
                PARTITION BY asset_id ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
            )
        END AS rolling_vol_30d
    FROM with_returns
),
btc AS (
    SELECT d.date, f.close AS btc_close
    FROM {{ ref('fact_daily_price') }} f
    JOIN {{ ref('dim_date') }}  d ON d.date_id = f.date_id
    JOIN {{ ref('dim_asset') }} a ON a.asset_id = f.asset_id
    WHERE a.symbol = 'BTC'
)
SELECT
    r.asset_id,
    r.date_id,
    r.daily_return,
    r.log_return,
    r.rolling_return_7d,
    r.rolling_return_30d,
    r.rolling_return_90d,
    r.rolling_return_180d,
    r.rolling_return_365d,
    r.rolling_vol_30d,
    (r.close / NULLIF(FIRST_VALUE(r.close) OVER (PARTITION BY r.asset_id ORDER BY r.date), 0)
     - b.btc_close / NULLIF(FIRST_VALUE(b.btc_close) OVER (ORDER BY b.date), 0)
    ) AS rel_perf_vs_btc
FROM rolling r
LEFT JOIN btc b ON b.date = r.date
