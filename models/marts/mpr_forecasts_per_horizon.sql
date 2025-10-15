
{{ config(
    materialized='table'
) }}

/*
This model calculates forecast errors for Norges Bank's Monetary Policy Report (MPR) forecasts of CPI, CPI-ATE, and policy rate.
It joins MPR forecasts with actual outcomes and computes the forecast horizon in months, enabling analysis of forecast accuracy by horizon.
Useful for creating histograms or summary statistics of forecast errors per horizon.
*/

WITH published AS (
    SELECT raport, year_month_published
    FROM {{ ref('mpr_publication_dates') }}
)
-- CPI forecast
, mpr_cpi AS (
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

-- CPI-ATE forecast
, mpr_cpi_ate AS (
    SELECT 
        mpr.year_month AS forecast_target,
        mpr.source,
        'cpi_ate' AS indicator,
        mpr.value AS forecast_value,
        pub.year_month_published,
        (CAST(SUBSTRING(mpr.year_month, 1, 4) AS INT) * 12 
         + CAST(SUBSTRING(mpr.year_month, 6, 2) AS INT))
        - (CAST(SUBSTRING(pub.year_month_published, 1, 4) AS INT) * 12 
         + CAST(SUBSTRING(pub.year_month_published, 6, 2) AS INT)) 
        AS forecast_horizon_months
    FROM {{ source('raw_nb', 'nb_mpr_indicators') }} mpr
    INNER JOIN published pub ON mpr.source = pub.raport
    WHERE mpr.indicator = 'cpi_ate'
        AND mpr.year_month > pub.year_month_published
)
, mpr_policy_rate AS (
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
    WHERE mpr.indicator = 'policy_rate'
        AND mpr.year_month > pub.year_month_published
)
-- CPI actuals
, cpi AS (
    SELECT
        period_code,
        indicator,
        value AS actual_value
    FROM {{ ref('indicators') }}
    WHERE measure_type = 'yearly_change'
        AND indicator = 'CPI'
)

-- CPI-ATE actuals
, cpi_ate AS (
    SELECT
        period_code,
        'cpi_ate' AS indicator,
        value AS actual_value
    FROM {{ ref('indicators') }}
    WHERE measure_type = 'yearly_change'
        AND indicator = 'CPI-ATE'
),
policy_rate_actuals AS (
    SELECT 
        year_month AS period_code,
        'policy_rate' AS indicator,
        rate AS actual_value
    FROM nb_policy_rates_monthly 
    WHERE tenor = 'SD'
)

SELECT
    mpr.source,
    mpr.forecast_horizon_months,
    mpr.forecast_target,
    mpr.indicator,
    mpr.forecast_value,
    cpi.actual_value,
    cpi.actual_value - mpr.forecast_value AS forecast_error,
    mpr.year_month_published
FROM mpr_cpi mpr
INNER JOIN cpi ON cpi.period_code = mpr.forecast_target

UNION ALL

SELECT
    mpr.source,
    mpr.forecast_horizon_months,
    mpr.forecast_target,
    mpr.indicator,
    mpr.forecast_value,
    cpi_ate.actual_value,
    cpi_ate.actual_value - mpr.forecast_value AS forecast_error,
    mpr.year_month_published
FROM mpr_cpi_ate mpr
INNER JOIN cpi_ate ON cpi_ate.period_code = mpr.forecast_target

UNION ALL

SELECT
    mpr.source,
    mpr.forecast_horizon_months,
    mpr.forecast_target,
    mpr.indicator,
    mpr.forecast_value,
    pra.actual_value,
    pra.actual_value - mpr.forecast_value AS forecast_error,
    mpr.year_month_published
FROM mpr_policy_rate mpr
INNER JOIN policy_rate_actuals pra ON pra.period_code = mpr.forecast_target
