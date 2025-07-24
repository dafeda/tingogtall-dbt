{{ config(materialized='table') }}

WITH gdp_data AS (
    SELECT 
        year_quarter,
        gdp_type,
        value_nok_million
    FROM {{ source('raw_ssb', 'ssb_09190_gdp_quarterly') }}
    WHERE gdp_type = 'mainland'
),

-- Calculate 8-quarter moving average trend
gdp_with_trend AS (
    SELECT 
        year_quarter,
        gdp_type,
        value_nok_million,
        -- 8-quarter (2-year) moving average trend
        AVG(value_nok_million) OVER (
            ORDER BY year_quarter 
            ROWS BETWEEN 7 PRECEDING AND CURRENT ROW
        ) AS trend_gdp_ma8,
        -- Calculate output gap: Y = 100(Y - Y*)/Y*
        100.0 * (
            value_nok_million - AVG(value_nok_million) OVER (
                ORDER BY year_quarter 
                ROWS BETWEEN 7 PRECEDING AND CURRENT ROW
            )
        ) / AVG(value_nok_million) OVER (
            ORDER BY year_quarter 
            ROWS BETWEEN 7 PRECEDING AND CURRENT ROW
        ) AS output_gap_percent
    FROM gdp_data
),

cpi_data AS (
    SELECT 
        year_month,
        value as inflation_rate
    FROM {{ source('raw_ssb', 'ssb_03013_cpi_by_group') }}
    WHERE consumption_group = 'TOTAL' 
    AND measure_type = 'yearly_change'
),

policy_rate_data AS (
    SELECT 
        year_month,
        value as policy_rate
    FROM {{ source('raw_nb', 'nb_mpr_indicators') }}
    WHERE indicator = 'policy_rate'
    AND source = 'PPR 2/25'
)

SELECT 
    g.year_quarter,
    g.gdp_type,
    g.value_nok_million,
    g.trend_gdp_ma8,
    g.output_gap_percent,
    c.inflation_rate,
    p.policy_rate,
    -- Taylor Rule: r = p + 0.5*y + 0.5*(p - target) + equilibrium_real_rate
    -- Using 2.0% inflation target and 2.0% equilibrium real rate (adjust for Norway as needed)
    c.inflation_rate + 0.5 * g.output_gap_percent + 0.5 * (c.inflation_rate - 2.0) + 2.0 AS taylor_rule_rate,
    -- Calculate deviation of actual policy rate from Taylor rule
    p.policy_rate - (c.inflation_rate + 0.5 * g.output_gap_percent + 0.5 * (c.inflation_rate - 2.0) + 2.0) AS policy_deviation
FROM gdp_with_trend g
INNER JOIN cpi_data c ON 
    -- Map GDP quarters to CPI months for temporal alignment
    -- We use the MIDDLE month of each quarter because:
    -- - CPI is published monthly and represents point-in-time inflation
    -- - GDP represents the AVERAGE economic activity over 3 months
    -- - Middle month best approximates the quarterly average
    -- Q1 (Jan-Mar) → Feb, Q2 (Apr-Jun) → May, Q3 (Jul-Sep) → Aug, Q4 (Oct-Dec) → Nov
    CASE 
        WHEN RIGHT(g.year_quarter, 2) = 'K1' THEN LEFT(g.year_quarter, 4) || 'M02'
        WHEN RIGHT(g.year_quarter, 2) = 'K2' THEN LEFT(g.year_quarter, 4) || 'M05'
        WHEN RIGHT(g.year_quarter, 2) = 'K3' THEN LEFT(g.year_quarter, 4) || 'M08'
        WHEN RIGHT(g.year_quarter, 2) = 'K4' THEN LEFT(g.year_quarter, 4) || 'M11'
    END = c.year_month
INNER JOIN policy_rate_data p ON
    -- Map GDP quarters to policy rate announcement months
    -- Policy rates are announced at END of each quarter when Norges Bank publishes MPR:
    -- - March MPR sets policy for Q1 (announced after Q1 GDP data is known)
    -- - June MPR sets policy for Q2, September MPR for Q3, December MPR for Q4
    -- This reflects the real-world timing of monetary policy decisions
    CASE 
        WHEN RIGHT(g.year_quarter, 2) = 'K1' THEN LEFT(g.year_quarter, 4) || 'M03'
        WHEN RIGHT(g.year_quarter, 2) = 'K2' THEN LEFT(g.year_quarter, 4) || 'M06'
        WHEN RIGHT(g.year_quarter, 2) = 'K3' THEN LEFT(g.year_quarter, 4) || 'M09'
        WHEN RIGHT(g.year_quarter, 2) = 'K4' THEN LEFT(g.year_quarter, 4) || 'M12'
    END = p.year_month
ORDER BY g.year_quarter
