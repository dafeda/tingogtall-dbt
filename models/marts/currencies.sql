{{ config(
  materialized='table',
) }}

WITH deduped AS (
    SELECT DISTINCT year_month, currency, value
    FROM {{ source('stage', 'currencies') }}
    WHERE value IS NOT NULL AND value != 0
),

base_rates AS (
    SELECT
        year_month,
        currency AS base_currency,
        'NOK' AS quote_currency,
        value
    FROM deduped
    WHERE currency != 'NOK'
),

inverse_rates AS (
    SELECT
        year_month,
        'NOK' AS base_currency,
        currency AS quote_currency,
        1.0 / value AS value
    FROM deduped
    WHERE currency != 'NOK'
),

cross_pairs AS (
    SELECT
        a.year_month,
        a.currency AS base_currency,
        b.currency AS quote_currency,
        a.value / b.value AS value
    FROM deduped a
    JOIN deduped b
        ON a.year_month = b.year_month
    WHERE a.currency != b.currency
      AND a.currency != 'NOK'
      AND b.currency != 'NOK'
),

all_pairs AS (
    SELECT year_month, base_currency, quote_currency, value FROM base_rates
    UNION ALL
    SELECT year_month, base_currency, quote_currency, value FROM inverse_rates
    UNION ALL
    SELECT year_month, base_currency, quote_currency, value FROM cross_pairs
),

lagged AS (
    SELECT
        *,
        LAG(value, 12) OVER (PARTITION BY base_currency, quote_currency ORDER BY year_month) AS prev_value
    FROM all_pairs
),

yearly_changes AS (
    SELECT
        year_month,
        base_currency,
        quote_currency,
        CASE
            WHEN prev_value IS NOT NULL
                THEN ((value - prev_value) / prev_value) * 100
        END AS value
    FROM lagged
)

SELECT
    year_month,
    base_currency,
    quote_currency,
    'index' AS measure_type,
    value
FROM all_pairs

UNION ALL

SELECT
    year_month,
    base_currency,
    quote_currency,
    'yearly_change' AS measure_type,
    value
FROM yearly_changes

ORDER BY base_currency, quote_currency, year_month, measure_type
