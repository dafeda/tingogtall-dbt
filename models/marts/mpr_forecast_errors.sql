{{ config(materialized='table') }}

/*
This model analyzes Norges Bank CPI and policy rate forecast errors to create chart-ready uncertainty bands.
It generates P10 and P90 confidence intervals following Norges Bank's standard presentation.

The model:
1. Calculates historical forecast errors by comparing MPR predictions to actual data
2. Generates error bands based on percentiles of historical forecast errors
3. Applies these bands to the latest forecasts to create uncertainty intervals

*/

WITH published AS (
    SELECT
        raport,
        year_month_published
    FROM {{ ref('mpr_publication_dates') }}
),

mpr_cpi AS (
    SELECT
        mpr_cpi.year_month AS forecast_target,
        mpr_cpi.source,
        mpr_cpi.indicator,
        mpr_cpi.value AS forecast_value,
        pub.year_month_published
    FROM {{ source('raw_nb', 'nb_mpr_indicators') }} AS mpr_cpi
    INNER JOIN published AS pub ON mpr_cpi.source = pub.raport
    WHERE
        mpr_cpi.indicator = 'cpi'
        AND mpr_cpi.year_month > pub.year_month_published
),

mpr_cpi_ate AS (
    SELECT
        mpr_cpi_ate.year_month AS forecast_target,
        mpr_cpi_ate.source,
        mpr_cpi_ate.indicator,
        mpr_cpi_ate.value AS forecast_value,
        pub.year_month_published
    FROM {{ source('raw_nb', 'nb_mpr_indicators') }} AS mpr_cpi_ate
    INNER JOIN published AS pub ON mpr_cpi_ate.source = pub.raport
    WHERE
        mpr_cpi_ate.indicator = 'cpi_ate'
        AND mpr_cpi_ate.year_month > pub.year_month_published
),

mpr_policy_rate AS (
    SELECT
        mpr.year_month AS forecast_target,
        mpr.source,
        mpr.indicator,
        mpr.value AS forecast_value,
        pub.year_month_published
    FROM {{ source('raw_nb', 'nb_mpr_indicators') }} AS mpr
    INNER JOIN published AS pub ON mpr.source = pub.raport
    WHERE
        mpr.indicator = 'policy_rate'
        AND mpr.year_month > pub.year_month_published
),

cpi AS (
    SELECT
        period_code,
        indicator,
        value AS actual_value
    FROM {{ ref('indicators') }}
    WHERE
        measure_type = 'yearly_change'
        AND indicator = 'CPI'
),

cpi_ate AS (
    SELECT
        period_code,
        indicator,
        value AS actual_value
    FROM {{ ref('indicators') }}
    WHERE
        measure_type = 'yearly_change'
        AND indicator = 'CPI-ATE'
),

policy_rate_actuals AS (
    SELECT
        year_month AS period_code,
        'policy_rate' AS indicator,
        rate AS actual_value
    FROM {{ source('raw_nb', 'nb_policy_rates_monthly') }}
    WHERE tenor = 'SD'
),

forecast_errors_cpi AS (
    SELECT
        mpr_cpi.forecast_target AS year_month,
        mpr_cpi.source AS forecast_source,
        mpr_cpi.year_month_published,

        cpi.indicator,

        cpi.actual_value,
        mpr_cpi.forecast_value,
        (
            CAST(SUBSTRING(mpr_cpi.forecast_target, 1, 4) AS INT) * 12
            + CAST(SUBSTRING(mpr_cpi.forecast_target, 6, 2) AS INT)
        )
        - (
            CAST(SUBSTRING(mpr_cpi.year_month_published, 1, 4) AS INT) * 12
            + CAST(SUBSTRING(mpr_cpi.year_month_published, 6, 2) AS INT)
        )
        AS forecast_horizon_months,
        cpi.actual_value - mpr_cpi.forecast_value AS forecast_error,

        CASE
            WHEN cpi.actual_value IS NULL THEN 'Not yet available'
            ELSE 'Available'
        END AS actual_status
    FROM mpr_cpi
    INNER JOIN cpi ON mpr_cpi.forecast_target = cpi.period_code
),

forecast_errors_cpi_ate AS (
    SELECT
        mpr_cpi_ate.forecast_target AS year_month,
        mpr_cpi_ate.source AS forecast_source,
        mpr_cpi_ate.year_month_published,

        cpi_ate.indicator,

        cpi_ate.actual_value,
        mpr_cpi_ate.forecast_value,
        (
            CAST(SUBSTRING(mpr_cpi_ate.forecast_target, 1, 4) AS INT) * 12
            + CAST(SUBSTRING(mpr_cpi_ate.forecast_target, 6, 2) AS INT)
        )
        - (
            CAST(SUBSTRING(mpr_cpi_ate.year_month_published, 1, 4) AS INT) * 12
            + CAST(SUBSTRING(mpr_cpi_ate.year_month_published, 6, 2) AS INT)
        )
        AS forecast_horizon_months,
        cpi_ate.actual_value - mpr_cpi_ate.forecast_value AS forecast_error,

        CASE
            WHEN cpi_ate.actual_value IS NULL THEN 'Not yet available'
            ELSE 'Available'
        END AS actual_status
    FROM mpr_cpi_ate
    INNER JOIN cpi_ate ON mpr_cpi_ate.forecast_target = cpi_ate.period_code
),

forecast_errors_policy_rate AS (
    SELECT
        mpr_policy_rate.forecast_target AS year_month,
        mpr_policy_rate.source AS forecast_source,
        mpr_policy_rate.year_month_published,

        policy_rate_actuals.indicator,

        policy_rate_actuals.actual_value,
        mpr_policy_rate.forecast_value,
        (
            CAST(SUBSTRING(mpr_policy_rate.forecast_target, 1, 4) AS INT) * 12
            + CAST(SUBSTRING(mpr_policy_rate.forecast_target, 6, 2) AS INT)
        )
        - (
            CAST(SUBSTRING(mpr_policy_rate.year_month_published, 1, 4) AS INT)
            * 12
            + CAST(SUBSTRING(mpr_policy_rate.year_month_published, 6, 2) AS INT)
        )
        AS forecast_horizon_months,
        policy_rate_actuals.actual_value
        - mpr_policy_rate.forecast_value AS forecast_error,

        CASE
            WHEN
                policy_rate_actuals.actual_value IS NULL
                THEN 'Not yet available'
            ELSE 'Available'
        END AS actual_status
    FROM mpr_policy_rate
    INNER JOIN
        policy_rate_actuals
        ON mpr_policy_rate.forecast_target = policy_rate_actuals.period_code
),

error_bands_cpi AS (
    SELECT
        forecast_horizon_months,
        COUNT(*) AS n_observations,
        -- 80% confidence interval (10th to 90th percentile)
        PERCENTILE_CONT(0.10) WITHIN GROUP (
            ORDER BY forecast_error
        ) AS p10_error,
        PERCENTILE_CONT(0.90) WITHIN GROUP (
            ORDER BY forecast_error
        ) AS p90_error,
        AVG(forecast_error) AS mean_error
    FROM forecast_errors_cpi
    WHERE actual_status = 'Available'
    GROUP BY forecast_horizon_months
),

error_bands_cpi_ate AS (
    SELECT
        forecast_horizon_months,
        COUNT(*) AS n_observations,
        -- 80% confidence interval (10th to 90th percentile)
        PERCENTILE_CONT(0.10) WITHIN GROUP (
            ORDER BY forecast_error
        ) AS p10_error,
        PERCENTILE_CONT(0.90) WITHIN GROUP (
            ORDER BY forecast_error
        ) AS p90_error,
        AVG(forecast_error) AS mean_error
    FROM forecast_errors_cpi_ate
    WHERE actual_status = 'Available'
    GROUP BY forecast_horizon_months
),

error_bands_policy_rate AS (
    SELECT
        forecast_horizon_months,
        COUNT(*) AS n_observations,
        -- 80% confidence interval (10th to 90th percentile)
        PERCENTILE_CONT(0.10) WITHIN GROUP (
            ORDER BY forecast_error
        ) AS p10_error,
        PERCENTILE_CONT(0.90) WITHIN GROUP (
            ORDER BY forecast_error
        ) AS p90_error,
        AVG(forecast_error) AS mean_error
    FROM forecast_errors_policy_rate
    WHERE actual_status = 'Available'
    GROUP BY forecast_horizon_months
),

forecast_with_bands_cpi AS (
    SELECT
        mpr_cpi.year_month,
        mpr_cpi.value AS forecast,
        pub.year_month_published,

        e.n_observations AS historical_obs_for_this_horizon,

        -- 80% confidence interval (P10-P90)
        (
            CAST(SUBSTRING(mpr_cpi.year_month, 1, 4) AS INT) * 12
            + CAST(SUBSTRING(mpr_cpi.year_month, 6, 2) AS INT)
        )
        - (
            CAST(SUBSTRING(pub.year_month_published, 1, 4) AS INT) * 12
            + CAST(SUBSTRING(pub.year_month_published, 6, 2) AS INT)
        )
        AS forecast_horizon_months,
        mpr_cpi.value + e.p10_error AS lower_80,
        mpr_cpi.value + e.p90_error AS upper_80

    FROM {{ source('raw_nb', 'nb_mpr_indicators') }} AS mpr_cpi
    INNER JOIN published AS pub ON mpr_cpi.source = pub.raport
    LEFT JOIN error_bands_cpi AS e
        ON (
            (
                CAST(SUBSTRING(mpr_cpi.year_month, 1, 4) AS INT) * 12
                + CAST(SUBSTRING(mpr_cpi.year_month, 6, 2) AS INT)
            )
            - (
                CAST(SUBSTRING(pub.year_month_published, 1, 4) AS INT) * 12
                + CAST(SUBSTRING(pub.year_month_published, 6, 2) AS INT)
            )
        ) = e.forecast_horizon_months
    WHERE
        mpr_cpi.indicator = 'cpi'
        AND pub.year_month_published
        = (SELECT MAX(year_month_published) FROM published)
        AND mpr_cpi.year_month > pub.year_month_published
),

forecast_with_bands_cpi_ate AS (
    SELECT
        mpr_cpi_ate.year_month,
        mpr_cpi_ate.value AS forecast,
        pub.year_month_published,

        e.n_observations AS historical_obs_for_this_horizon,

        -- 80% confidence interval (P10-P90)
        (
            CAST(SUBSTRING(mpr_cpi_ate.year_month, 1, 4) AS INT) * 12
            + CAST(SUBSTRING(mpr_cpi_ate.year_month, 6, 2) AS INT)
        )
        - (
            CAST(SUBSTRING(pub.year_month_published, 1, 4) AS INT) * 12
            + CAST(SUBSTRING(pub.year_month_published, 6, 2) AS INT)
        )
        AS forecast_horizon_months,
        mpr_cpi_ate.value + e.p10_error AS lower_80,
        mpr_cpi_ate.value + e.p90_error AS upper_80

    FROM {{ source('raw_nb', 'nb_mpr_indicators') }} AS mpr_cpi_ate
    INNER JOIN published AS pub ON mpr_cpi_ate.source = pub.raport
    LEFT JOIN error_bands_cpi_ate AS e
        ON (
            (
                CAST(SUBSTRING(mpr_cpi_ate.year_month, 1, 4) AS INT) * 12
                + CAST(SUBSTRING(mpr_cpi_ate.year_month, 6, 2) AS INT)
            )
            - (
                CAST(SUBSTRING(pub.year_month_published, 1, 4) AS INT) * 12
                + CAST(SUBSTRING(pub.year_month_published, 6, 2) AS INT)
            )
        ) = e.forecast_horizon_months
    WHERE
        mpr_cpi_ate.indicator = 'cpi_ate'
        AND pub.year_month_published
        = (SELECT MAX(year_month_published) FROM published)
        AND mpr_cpi_ate.year_month > pub.year_month_published
),

forecast_with_bands_policy_rate AS (
    SELECT
        mpr.year_month,
        mpr.value AS forecast,
        pub.year_month_published,

        e.n_observations AS historical_obs_for_this_horizon,

        -- 80% confidence interval (P10-P90)
        (
            CAST(SUBSTRING(mpr.year_month, 1, 4) AS INT) * 12
            + CAST(SUBSTRING(mpr.year_month, 6, 2) AS INT)
        )
        - (
            CAST(SUBSTRING(pub.year_month_published, 1, 4) AS INT) * 12
            + CAST(SUBSTRING(pub.year_month_published, 6, 2) AS INT)
        )
        AS forecast_horizon_months,
        mpr.value + e.p10_error AS lower_80,
        mpr.value + e.p90_error AS upper_80

    FROM {{ source('raw_nb', 'nb_mpr_indicators') }} AS mpr
    INNER JOIN published AS pub ON mpr.source = pub.raport
    LEFT JOIN error_bands_policy_rate AS e
        ON (
            (
                CAST(SUBSTRING(mpr.year_month, 1, 4) AS INT) * 12
                + CAST(SUBSTRING(mpr.year_month, 6, 2) AS INT)
            )
            - (
                CAST(SUBSTRING(pub.year_month_published, 1, 4) AS INT) * 12
                + CAST(SUBSTRING(pub.year_month_published, 6, 2) AS INT)
            )
        ) = e.forecast_horizon_months
    WHERE
        mpr.indicator = 'policy_rate'
        AND pub.year_month_published
        = (SELECT MAX(year_month_published) FROM published)
        AND mpr.year_month > pub.year_month_published
)

SELECT
    year_month,
    'cpi' AS indicator,
    forecast_horizon_months,
    historical_obs_for_this_horizon,
    'forecast' AS series,
    forecast AS value
FROM forecast_with_bands_cpi

UNION ALL

SELECT
    year_month,
    'cpi' AS indicator,
    forecast_horizon_months,
    historical_obs_for_this_horizon,
    'p10' AS series,
    lower_80 AS value
FROM forecast_with_bands_cpi

UNION ALL

SELECT
    year_month,
    'cpi' AS indicator,
    forecast_horizon_months,
    historical_obs_for_this_horizon,
    'p90' AS series,
    upper_80 AS value
FROM forecast_with_bands_cpi

UNION ALL

SELECT
    year_month,
    'policy_rate' AS indicator,
    forecast_horizon_months,
    historical_obs_for_this_horizon,
    'forecast' AS series,
    forecast AS value
FROM forecast_with_bands_policy_rate

UNION ALL

SELECT
    year_month,
    'policy_rate' AS indicator,
    forecast_horizon_months,
    historical_obs_for_this_horizon,
    'p10' AS series,
    lower_80 AS value
FROM forecast_with_bands_policy_rate

UNION ALL

SELECT
    year_month,
    'policy_rate' AS indicator,
    forecast_horizon_months,
    historical_obs_for_this_horizon,
    'p90' AS series,
    upper_80 AS value
FROM forecast_with_bands_policy_rate

UNION ALL

SELECT
    year_month,
    'cpi_ate' AS indicator,
    forecast_horizon_months,
    historical_obs_for_this_horizon,
    'forecast' AS series,
    forecast AS value
FROM forecast_with_bands_cpi_ate

UNION ALL

SELECT
    year_month,
    'cpi_ate' AS indicator,
    forecast_horizon_months,
    historical_obs_for_this_horizon,
    'p10' AS series,
    lower_80 AS value
FROM forecast_with_bands_cpi_ate

UNION ALL

SELECT
    year_month,
    'cpi_ate' AS indicator,
    forecast_horizon_months,
    historical_obs_for_this_horizon,
    'p90' AS series,
    upper_80 AS value
FROM forecast_with_bands_cpi_ate

ORDER BY indicator, year_month, series
