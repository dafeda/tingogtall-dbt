{{ config(materialized='table') }}
WITH cpi_ate_data AS (
   SELECT 
       year_month,
       value as inflation_rate
   FROM {{ source('raw_nb', 'nb_mpr_indicators') }}
   WHERE indicator = 'cpi_ate'
   AND source = 'PPR 2/25'
),
policy_rate_data AS (
   SELECT 
       year_month,
       value as policy_rate
   FROM {{ source('raw_nb', 'nb_mpr_indicators') }}
   WHERE indicator = 'policy_rate'
   AND source = 'PPR 2/25'
),
output_gap_data AS (
   SELECT 
       year_month,
       value as output_gap
   FROM {{ source('raw_nb', 'nb_mpr_indicators') }}
   WHERE indicator = 'output_gap'
   AND source = 'PPR 2/25'
)
SELECT 
   c.year_month,
   c.inflation_rate,
   og.output_gap,
   p.policy_rate,
   -- Taylor Rule: r = p + 0.5*y + 0.5*(p - target) + equilibrium_real_rate
   -- Using 2.0% inflation target and 0.5% equilibrium real rate
   -- Source: https://www.norges-bank.no/aktuelt/publikasjoner/Staff-Memo/2022/sm-7-2022/
   c.inflation_rate + 0.5 * og.output_gap + 0.5 * (c.inflation_rate - 2.0) + 0.5 AS taylor_rule_rate,
   -- Calculate deviation of actual policy rate from Taylor rule
   p.policy_rate - (c.inflation_rate + 0.5 * og.output_gap + 0.5 * (c.inflation_rate - 2.0) + 0.5) AS policy_deviation
FROM cpi_ate_data c
INNER JOIN policy_rate_data p ON c.year_month = p.year_month
INNER JOIN output_gap_data og ON c.year_month = og.year_month
ORDER BY c.year_month
