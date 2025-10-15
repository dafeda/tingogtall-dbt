{{ config(materialized='table') }}

/*
This model analyzes Norges Bank CPI forecast errors to create chart-ready uncertainty bands.
It generates 50% and 70% confidence intervals following Norges Bank's standard presentation.

The model:
1. Calculates historical forecast errors by comparing MPR predictions to actual CPI data
2. Generates error bands based on percentiles of historical forecast errors
3. Applies these bands to the latest forecasts to create uncertainty intervals

Output format is in tidy/long format with columns:
- year_month: forecast target date
- year_month_published: when forecast was published
- forecast_horizon_months: how many months ahead
- series: type of value (forecast, lower_50, upper_50, lower_70, upper_70)
- value: the CPI percentage value
- historical_obs_for_this_horizon: number of historical observations used

MPR Publication dates:
- MPR 2/24: Published 20 June 2024 10:00
- MPR 3/24: Published 19 September 2024 10:00
- MPR 4/24: Published 19 December 2024 10:00
- MPR 1/25: Published 27 March 2025 10:00
- MPR 2/25: Published 19 June 2025 10:00
- MPR 3/25: Published 18 September 2025 10:00
*/

WITH published AS (
    SELECT 'MPR 2/24' AS raport, '2024M06' AS year_month_published
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
, mpr AS (
    SELECT 
        mpr.year_month AS forecast_target,
        mpr.source,
        mpr.indicator,
        mpr.value AS forecast_value,
        pub.year_month_published
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
    SELECT
        mpr.forecast_target AS year_month,
        mpr.source AS forecast_source,
        mpr.year_month_published,
        
        (CAST(SUBSTRING(mpr.forecast_target, 1, 4) AS INT) * 12 
         + CAST(SUBSTRING(mpr.forecast_target, 6, 2) AS INT))
        - (CAST(SUBSTRING(mpr.year_month_published, 1, 4) AS INT) * 12 
         + CAST(SUBSTRING(mpr.year_month_published, 6, 2) AS INT)) 
        AS forecast_horizon_months,
        
        ind.indicator,
        ind.actual_value,
        mpr.forecast_value,
        ind.actual_value - mpr.forecast_value AS forecast_error,
        
        CASE 
            WHEN ind.actual_value IS NULL THEN 'Not yet available'
            ELSE 'Available'
        END AS actual_status
    FROM mpr
    INNER JOIN ind ON mpr.forecast_target = ind.period_code
)
, error_bands AS (
    SELECT 
        forecast_horizon_months,
        COUNT(*) AS n_observations,
        
        -- 50% confidence interval (25th to 75th percentile) - DARKER BAND
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY forecast_error) AS p25_error,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY forecast_error) AS p75_error,
        
        -- 70% confidence interval (15th to 85th percentile) - LIGHTER BAND
        PERCENTILE_CONT(0.15) WITHIN GROUP (ORDER BY forecast_error) AS p15_error,
        PERCENTILE_CONT(0.85) WITHIN GROUP (ORDER BY forecast_error) AS p85_error,
        
        AVG(forecast_error) AS mean_error
    FROM forecast_errors
    WHERE actual_status = 'Available'
    GROUP BY forecast_horizon_months
)
, forecast_with_bands AS (
    SELECT 
        mpr.year_month,
        mpr.value AS forecast,
        pub.year_month_published,
        
        (CAST(SUBSTRING(mpr.year_month, 1, 4) AS INT) * 12 
         + CAST(SUBSTRING(mpr.year_month, 6, 2) AS INT))
        - (CAST(SUBSTRING(pub.year_month_published, 1, 4) AS INT) * 12 
         + CAST(SUBSTRING(pub.year_month_published, 6, 2) AS INT)) 
        AS forecast_horizon_months,
        
        -- 50% confidence interval (DARKER shading)
        mpr.value + e.p25_error AS lower_50,
        mpr.value + e.p75_error AS upper_50,
        
        -- 70% confidence interval (LIGHTER shading)
        mpr.value + e.p15_error AS lower_70,
        mpr.value + e.p85_error AS upper_70,
        
        e.n_observations AS historical_obs_for_this_horizon
        
    FROM {{ source('raw_nb', 'nb_mpr_indicators') }} mpr
    INNER JOIN published pub ON mpr.source = pub.raport
    LEFT JOIN error_bands e ON (
        (CAST(SUBSTRING(mpr.year_month, 1, 4) AS INT) * 12 
         + CAST(SUBSTRING(mpr.year_month, 6, 2) AS INT))
        - (CAST(SUBSTRING(pub.year_month_published, 1, 4) AS INT) * 12 
         + CAST(SUBSTRING(pub.year_month_published, 6, 2) AS INT))
    ) = e.forecast_horizon_months
    WHERE mpr.indicator = 'cpi'
        AND pub.year_month_published = (SELECT MAX(year_month_published) FROM published)
        AND mpr.year_month > pub.year_month_published
)
SELECT 
    year_month,
    forecast_horizon_months,
    historical_obs_for_this_horizon,
    'forecast' AS series,
    forecast AS value
FROM forecast_with_bands

UNION ALL

SELECT 
    year_month,
    forecast_horizon_months,
    historical_obs_for_this_horizon,
    'lower_50' AS series,
    lower_50 AS value
FROM forecast_with_bands

UNION ALL

SELECT 
    year_month,
    forecast_horizon_months,
    historical_obs_for_this_horizon,
    'upper_50' AS series,
    upper_50 AS value
FROM forecast_with_bands

UNION ALL

SELECT 
    year_month,
    forecast_horizon_months,
    historical_obs_for_this_horizon,
    'lower_70' AS series,
    lower_70 AS value
FROM forecast_with_bands

UNION ALL

SELECT 
    year_month,
    forecast_horizon_months,
    historical_obs_for_this_horizon,
    'upper_70' AS series,
    upper_70 AS value
FROM forecast_with_bands

ORDER BY year_month, series