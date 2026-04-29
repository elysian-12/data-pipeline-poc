{{ config(materialized='table') }}

-- Distinct trade dates from silver, widened with calendar attributes.
-- `is_trading_day` is authoritative from scripts/seed_dim_date.py (NYSE calendar).
-- This model joins that seed; if seed is empty, `is_trading_day` falls back to weekday ∈ [1..5].

WITH observed AS (
    SELECT DISTINCT date FROM {{ ref('stg_prices') }}
),
calendar AS (
    SELECT
        date,
        CAST(strftime(date, '%Y%m%d') AS BIGINT) AS date_id,
        EXTRACT(year    FROM date) AS year,
        EXTRACT(quarter FROM date) AS quarter,
        EXTRACT(month   FROM date) AS month,
        EXTRACT(day     FROM date) AS day,
        EXTRACT(dayofweek FROM date) AS dow
    FROM observed
)
SELECT
    c.date_id,
    c.date,
    c.year,
    c.quarter,
    c.month,
    c.day,
    c.dow,
    COALESCE(cal.is_trading_day, c.dow BETWEEN 1 AND 5) AS is_trading_day
FROM calendar c
LEFT JOIN {{ source('meta', 'nyse_calendar') }} cal ON cal.date = c.date
