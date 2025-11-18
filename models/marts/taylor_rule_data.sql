{{ config(materialized='table') }}

/*
This model implements the Taylor Rule as described in John Taylor's seminal 1993 paper
"Discretion versus policy rules in practice":
https://web.stanford.edu/~johntayl/Onlinepaperscombinedbyyear/1993/Discretion_versus_Policy_Rules_in_Practice.pdf

The Taylor Rule provides a guideline for setting interest rates based on inflation and
economic output. However, this implementation uses the output gap as calculated by
Norges Bank rather than Taylor's original output measure.

Norges Bank output gap estimates: Forecasting properties, reliability and cyclical sensitivity:
https://www.norges-bank.no/aktuelt/publikasjoner/Working-Papers/2020/72020/

Taylor Rule formula: r = p + 0.5*y + 0.5*(p - target) + equilibrium_real_rate
Where:
- r = recommended policy rate
- p = inflation rate
- y = output gap
- target = inflation target (2.0%)
https://www.norges-bank.no/en/topics/Monetary-policy/monetary-policy-strategy/
- equilibrium_real_rate = long-term real interest rate (0.5%)
https://www.norges-bank.no/en/news-events/publications/Staff-Memo/2022/sm-7-2022/
Note that the memo states that the equilibrium real rate ranges between -0.5 and 0.5.
Using 0.5 for now.
*/

WITH cpi_ate_data AS (
    SELECT
        year_month,
        value AS inflation_rate
    FROM {{ source('raw_nb', 'nb_mpr_indicators') }}
    WHERE
        indicator = 'cpi_ate'
        AND source = 'PPR 2/25'
),

policy_rate_data AS (
    SELECT
        year_month,
        value AS policy_rate
    FROM {{ source('raw_nb', 'nb_mpr_indicators') }}
    WHERE
        indicator = 'policy_rate'
        AND source = 'PPR 2/25'
),

output_gap_data AS (
    SELECT
        year_month,
        value AS output_gap
    FROM {{ source('raw_nb', 'nb_mpr_indicators') }}
    WHERE
        indicator = 'output_gap'
        AND source = 'PPR 2/25'
)

SELECT
    c.year_month,
    c.inflation_rate,
    og.output_gap,
    p.policy_rate,
    c.inflation_rate
    + 0.5 * og.output_gap
    + 0.5 * (c.inflation_rate - 2.0)
    + 0.5 AS taylor_rule_rate,
    -- Calculate deviation of actual policy rate from Taylor rule
    p.policy_rate
    - (
        c.inflation_rate
        + 0.5 * og.output_gap
        + 0.5 * (c.inflation_rate - 2.0)
        + 0.5
    ) AS policy_deviation
FROM cpi_ate_data AS c
INNER JOIN policy_rate_data AS p ON c.year_month = p.year_month
INNER JOIN output_gap_data AS og ON c.year_month = og.year_month
ORDER BY c.year_month
