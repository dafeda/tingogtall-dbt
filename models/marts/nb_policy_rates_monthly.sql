{{
  config(
    materialized='table'
  )
}}

SELECT
    tp.period_id,
    tp.period_code,
    s.instrument_type,
    s.tenor,
    s.rate
FROM {{ source('stage', 'nb_policy_rates_monthly') }} AS s
INNER JOIN {{ source('public', 'time_periods') }} AS tp
    ON s.year_month = tp.period_code
