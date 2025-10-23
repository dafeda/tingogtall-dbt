{{ config(
    materialized='table'
) }}

-- Use this to create histograms of errors per horizon as in the master thesis

WITH published AS (
    SELECT 'MPR 3/20' AS raport, '2020M09' AS year_month_published
    UNION ALL
    SELECT 'MPR 4/20', '2020M12'
    UNION ALL
    SELECT 'MPR 1/21', '2021M03'
    UNION ALL
    SELECT 'MPR 2/21', '2021M06'
    UNION ALL
    SELECT 'MPR 3/21', '2021M09'
    UNION ALL
    SELECT 'MPR 4/21', '2021M12'
    UNION ALL
    SELECT 'MPR 1/22', '2022M03'
    UNION ALL
    SELECT 'MPR 2/22', '2022M06'
    UNION ALL
    SELECT 'MPR 3/22', '2022M09'
    UNION ALL
    SELECT 'MPR 4/22', '2022M12'
    UNION ALL
    SELECT 'MPR 1/23', '2023M03'
    UNION ALL
    SELECT 'MPR 2/23', '2023M06'
    UNION ALL
    SELECT 'MPR 3/23', '2023M09'
    UNION ALL
    SELECT 'MPR 4/23', '2023M12'
    UNION ALL
    SELECT 'MPR 1/24', '2024M03'
    UNION ALL
    SELECT 'MPR 2/24', '2024M06'
    UNION ALL
    SELECT 'MPR 3/24', '2024M09'
    UNION ALL
    SELECT 'MPR 4/24', '2024M12'
    UNION ALL
    SELECT 'MPR 1/25', '2025M03'
    UNION ALL
    SELECT 'MPR 2/25', '2025M06'
    UNION ALL
    SELECT 'MPR 3/25', '2025M09'
)
, mpr_with_forecast_horizon AS (
    SELECT 
        mpr.year_month AS forecast_target,
        mpr.source,
        mpr.indicator,
        mpr.value AS forecast_value,
        pub.year_month_published,
        (CAST(SUBSTRING(mpr.year_month, 1, 4) AS INT) * 12 
         + CAST(SUBSTRING(mpr.year_month, 6, 2) AS INT))
        - (CAST(SUBSTRING(pub.year_month_published, 1, 4) AS INT) * 12 
         + CAST(SUBSTRING(pub.year_month_published, 6, 2) AS INT)) 
        AS forecast_horizon_months
    FROM {{ source('raw_nb', 'nb_mpr_indicators') }} mpr
    INNER JOIN published pub ON mpr.source = pub.raport
    WHERE mpr.indicator = 'cpi'
        AND mpr.year_month > pub.year_month_published
)
, ind AS (
    SELECT
        period_code,
        indicator,
        value AS actual_value
    FROM {{ ref('indicators') }}
    WHERE measure_type = 'yearly_change'
        AND indicator = 'CPI'
)
, forecast_errors AS (
select
mpr.source,
mpr.forecast_horizon_months,
mpr.forecast_target,
mpr.indicator,
mpr.forecast_value,
ind.actual_value,
ind.actual_value - mpr.forecast_value AS forecast_error,
mpr.year_month_published
from mpr_with_forecast_horizon mpr
INNER JOIN ind ON ind.period_code = mpr.forecast_target
)
select * from forecast_errors where forecast_horizon_months=3