{{
  config(
    materialized='table'
  )
}}

SELECT
    tp.period_id,
    tp.period_code,
    s.indicator_name,
    s.value AS value_nok_per_capita
FROM {{ source('stage', 'ssb_09842_gdp_per_capita') }} AS s
INNER JOIN {{ source('public', 'time_periods') }} AS tp
    ON s.year = tp.period_code
