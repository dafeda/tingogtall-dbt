{{
  config(
    materialized='table'
  )
}}

SELECT
    tp.period_id,
    tp.period_code,
    CASE s.derived_series
        WHEN 'CPI adjusted for tax changes (CPI-AT)' THEN 'KPI-JA'
        WHEN 'CPI adjusted for tax changes and excluding energy products (CPI-ATE)' THEN 'KPI-JAE'
        WHEN 'CPI excluding energy products (CPI-AE)' THEN 'KPI-JE'
        WHEN 'CPI excluding electricity (CPI-AEL)' THEN 'KPI-JEL'
        ELSE s.derived_series
    END AS consumption_group,
    CASE
        WHEN
            LOWER(s.content_label) LIKE '%index%'
            OR LOWER(s.content_label) LIKE '%indeks%'
            THEN 'index'
        WHEN
            LOWER(s.content_label) LIKE '%monthly%'
            OR LOWER(s.content_label) LIKE '%måneds%'
            THEN 'monthly_change'
        WHEN
            LOWER(s.content_label) LIKE '%12-month%'
            OR LOWER(s.content_label) LIKE '%tolvmåned%'
            OR LOWER(s.content_label) LIKE '%12 month%'
            THEN 'yearly_change'
    END AS measure_type,
    s.value
FROM {{ source('stage', 'ssb_14706_adjusted_cpi_by_group') }} AS s
INNER JOIN {{ source('public', 'time_periods') }} AS tp
    ON s.year_month = tp.period_code
WHERE
    s.value IS NOT NULL
    AND (
        LOWER(s.content_label) LIKE '%index%'
        OR LOWER(s.content_label) LIKE '%indeks%'
        OR LOWER(s.content_label) LIKE '%monthly%'
        OR LOWER(s.content_label) LIKE '%måneds%'
        OR LOWER(s.content_label) LIKE '%12-month%'
        OR LOWER(s.content_label) LIKE '%tolvmåned%'
        OR LOWER(s.content_label) LIKE '%12 month%'
    )
