{{ config(materialized='table') }}

WITH types AS (
    SELECT DISTINCT asset_type AS name FROM {{ ref('stg_prices') }}
)
SELECT
    ROW_NUMBER() OVER (ORDER BY name) AS asset_type_id,
    name
FROM types
