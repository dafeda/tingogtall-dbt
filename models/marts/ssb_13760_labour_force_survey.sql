{{
  config(
    materialized='table'
  )
}}

SELECT
    tp.period_id,
    tp.period_code,
    s.sex,
    s.age,
    CASE
        WHEN s.contents = 'Unemployment rate (LFS)' THEN 'unemployment_rate'
    END AS type_of_adjustment,
    s.type_of_adjustment AS contents,
    s.value
FROM {{ source('stage', 'ssb_13760_labour_force_survey') }} AS s
INNER JOIN {{ source('public', 'time_periods') }} AS tp
    ON s.year_month = tp.period_code
