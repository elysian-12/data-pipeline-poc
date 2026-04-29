-- Singular: rolling_return_30d uses a calendar-day window, not a row-offset window.
--
-- The rolling_return macro must use RANGE BETWEEN INTERVAL N DAY PRECEDING. This
-- guards against an accidental revert to LAG(close, N), which is a row offset
-- and therefore not cross-asset comparable (a 30-row return is ~6 calendar weeks
-- for stocks but exactly 30 days for daily-trading crypto).
--
-- For each (asset, date) D, the expected value is
--     close(D) / FIRST_VALUE(close in the trailing 30 calendar days inclusive of D) - 1
-- Returns rows where rolling_return_30d disagrees with the recomputed value by
-- more than 1e-12.

WITH expected AS (
    SELECT
        f.asset_id,
        f.date_id,
        d.date,
        f.close,
        FIRST_VALUE(f.close) OVER (
            PARTITION BY f.asset_id ORDER BY d.date
            RANGE BETWEEN INTERVAL '30 days' PRECEDING AND CURRENT ROW
        ) AS anchor_close
    FROM {{ ref('fact_daily_price') }} f
    JOIN {{ ref('dim_date') }} d ON d.date_id = f.date_id
),
recomputed AS (
    SELECT
        asset_id,
        date_id,
        (close / NULLIF(anchor_close, 0)) - 1.0 AS expected_rolling_return_30d
    FROM expected
)
SELECT
    m.asset_id,
    m.date_id,
    m.rolling_return_30d,
    r.expected_rolling_return_30d
FROM {{ ref('fact_daily_metrics') }} m
JOIN recomputed r ON r.asset_id = m.asset_id AND r.date_id = m.date_id
WHERE
    (m.rolling_return_30d IS NULL) <> (r.expected_rolling_return_30d IS NULL)
    OR ABS(COALESCE(m.rolling_return_30d, 0) - COALESCE(r.expected_rolling_return_30d, 0)) > 1e-12
