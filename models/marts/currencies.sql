{{ config(
  materialized='table',
) }}

WITH base_data AS (
    SELECT
        year_month,
        currency,
        'index' AS measure_type,
        value
    FROM {{ source('stage', 'currencies') }}
),

-- Calculate yearly changes
yearly_changes AS (
    SELECT
        year_month,
        currency,
        'yearly_change' AS measure_type,
        CASE
            WHEN
                LAG(value, 12)
                    OVER (PARTITION BY currency ORDER BY year_month)
                IS NOT NULL
                THEN (
                    (
                        value
                        - LAG(value, 12)
                            OVER (PARTITION BY currency ORDER BY year_month)
                    )
                    / LAG(value, 12)
                        OVER (PARTITION BY currency ORDER BY year_month)
                ) * 100
        END AS value
    FROM {{ source('stage', 'currencies') }}
)

-- Combine both datasets
SELECT
    year_month,
    currency,
    measure_type,
    value
FROM base_data
UNION ALL
SELECT
    year_month,
    currency,
    measure_type,
    value
FROM yearly_changes
ORDER BY currency, year_month, measure_type
