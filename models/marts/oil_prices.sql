{{ config(
  materialized='table',
) }}

WITH base_data AS (
    SELECT
        date,
        indicator,
        description,
        unit,
        value
    FROM {{ source('stage', 'fred_series_observations') }}
),

monthly_aggregated AS (
    SELECT
        indicator,
        description,
        unit,
        DATE_TRUNC('month', date) AS month_start_date,
        TO_CHAR(date, 'YYYY"M"MM') AS year_month,
        AVG(value) AS avg_value,
        MIN(value) AS min_value,
        MAX(value) AS max_value,
        COUNT(*) AS observation_count
    FROM base_data
    GROUP BY
        DATE_TRUNC('month', date),
        TO_CHAR(date, 'YYYY"M"MM'),
        indicator,
        description,
        unit
)

SELECT
    year_month,
    month_start_date,
    indicator,
    description,
    unit,
    avg_value,
    min_value,
    max_value,
    observation_count
FROM monthly_aggregated
ORDER BY year_month, indicator
