{{
  config(
    materialized='table'
  )
}}

SELECT
    tp.period_id,
    tp.period_code,
    s.consumption_group,
    CASE
        WHEN s.content_label = 'Consumer Price Index (2025=100)' THEN 'index'
        WHEN s.content_label = 'Monthly change (per cent)' THEN 'monthly_change'
        WHEN s.content_label = '12-month rate (per cent)' THEN 'yearly_change'
    END AS measure_type,
    s.value
FROM {{ source('stage', 'ssb_14700_cpi_by_group') }} AS s
INNER JOIN {{ source('public', 'time_periods') }} AS tp
    ON s.year_month = tp.period_code
