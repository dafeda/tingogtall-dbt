{{
  config(
    materialized='table'
  )
}}

SELECT
    tp.period_id,
    tp.period_code,
    CASE
        WHEN s.macroeconomic_indicator = 'Gross domestic product, market values' THEN 'total'
        WHEN s.macroeconomic_indicator = 'Gross domestic product Mainland Norway, market values' THEN 'mainland'
    END AS gdp_type,
    s.value AS value_nok_million
FROM {{ source('stage', 'ssb_09190_gdp_quarterly') }} AS s
INNER JOIN {{ source('public', 'time_periods') }} AS tp
    ON s.year_quarter = tp.period_code
