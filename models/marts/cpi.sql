{{ config(materialized='table') }}

WITH base AS (
    -- Eurostat HICP: monthly index values by country
    SELECT
        tp.period_type,
        tp.period_code,
        e.geo AS country,
        e.indicator,
        e.description,
        e.value,
        tp.start_date,
        tp.end_date
    FROM {{ source('stage', 'eurostat_series_observations') }} AS e
    INNER JOIN {{ source('public', 'time_periods') }} AS tp
        ON TO_CHAR(e.date, 'YYYY"M"MM') = tp.period_code

    UNION ALL

    -- Norway CPI (SSB 14700): total consumption group, index measure
    SELECT
        tp.period_type,
        tp.period_code,
        'NO' AS country,
        'CPI' AS indicator,
        'Consumer Price Index (2025=100)' AS description,
        s.value,
        tp.start_date,
        tp.end_date
    FROM {{ ref('ssb_14700_cpi_by_group') }} AS s
    INNER JOIN {{ source('public', 'time_periods') }} AS tp
        ON s.period_id = tp.period_id
    WHERE s.consumption_group = '00' AND s.measure_type = 'index'

    UNION ALL

    -- Norway adjusted CPI (SSB 14706): index measure only
    SELECT
        tp.period_type,
        tp.period_code,
        'NO' AS country,
        CASE s.consumption_group
            WHEN 'KPI-JA' THEN 'CPI-AT'
            WHEN 'KPI-JAE' THEN 'CPI-ATE'
            WHEN 'KPI-JE' THEN 'CPI-AE'
            WHEN 'KPI-JEL' THEN 'CPI-AEL'
            ELSE s.consumption_group
        END AS indicator,
        CASE s.consumption_group
            WHEN 'KPI-JA'
                THEN 'CPI adjusted for tax changes.'
            WHEN 'KPI-JAE'
                THEN 'CPI adjusted for tax changes and excluding energy products'
            WHEN 'KPI-JE'
                THEN 'CPI excluding energy products'
            WHEN 'KPI-JEL'
                THEN 'CPI excluding electricity'
            ELSE s.consumption_group
        END AS description,
        s.value,
        tp.start_date,
        tp.end_date
    FROM {{ ref('ssb_14706_adjusted_cpi_by_group') }} AS s
    INNER JOIN {{ source('public', 'time_periods') }} AS tp
        ON s.period_id = tp.period_id
    WHERE s.measure_type = 'index'

    UNION ALL

    -- US CPI (FRED CPIAUCSL): daily -> monthly average
    SELECT
        tp.period_type,
        tp.period_code,
        'US' AS country,
        'CPI' AS indicator,
        'Consumer Price Index for All Urban Consumers: All Items' AS description,
        AVG(f.value) AS value,
        tp.start_date,
        tp.end_date
    FROM {{ source('stage', 'fred_series_observations') }} AS f
    INNER JOIN {{ source('public', 'time_periods') }} AS tp
        ON TO_CHAR(f.date, 'YYYY"M"MM') = tp.period_code
    WHERE f.indicator = 'CPIAUCSL' AND f.value IS NOT NULL
    GROUP BY
        tp.period_type,
        tp.period_code,
        tp.start_date,
        tp.end_date
),

with_changes AS (
    SELECT
        *,
        ROUND(
            (
                (value - LAG(value, 12) OVER w)
                / NULLIF(LAG(value, 12) OVER w, 0)
            ) * 100,
            2
        ) AS yearly_change,
        ROUND(
            (
                (value - LAG(value, 1) OVER w)
                / NULLIF(LAG(value, 1) OVER w, 0)
            ) * 100,
            2
        ) AS monthly_change
    FROM base
    WINDOW w AS (
        PARTITION BY country, indicator
        ORDER BY start_date
    )
),

unpivoted AS (
    SELECT
        period_type,
        period_code,
        country,
        indicator,
        description,
        'index' AS measure_type,
        value,
        start_date,
        end_date
    FROM with_changes

    UNION ALL

    SELECT
        period_type,
        period_code,
        country,
        indicator,
        description,
        'yearly_change' AS measure_type,
        yearly_change AS value,
        start_date,
        end_date
    FROM with_changes
    WHERE yearly_change IS NOT NULL

    UNION ALL

    SELECT
        period_type,
        period_code,
        country,
        indicator,
        description,
        'monthly_change' AS measure_type,
        monthly_change AS value,
        start_date,
        end_date
    FROM with_changes
    WHERE monthly_change IS NOT NULL
)

SELECT
    period_type,
    period_code,
    country,
    indicator,
    description,
    measure_type,
    value,
    start_date,
    end_date
FROM unpivoted
ORDER BY
    country,
    indicator,
    period_code,
    measure_type
