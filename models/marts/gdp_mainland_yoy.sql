{{ config(materialized='table') }}
/*
Mainland GDP with year-over-year growth calculation.
Mainland GDP excludes petroleum activities and ocean transport, 
providing a better measure of the domestic economy's performance.
Uses 4-quarter lag to calculate year-over-year percentage change.

Quarter-to-month mapping uses end-of-quarter convention (Q1→Mar, Q2→Jun, etc.)
because GDP data represents the cumulative economic activity for the entire quarter
and is typically published after the quarter ends. This aligns with standard
economic reporting practices and ensures temporal consistency with other indicators.
*/
SELECT 
    tp.period_code AS year_quarter,
    -- Map quarter to end-of-quarter month for consistency with economic reporting standards
    CASE 
        WHEN RIGHT(tp.period_code, 2) = 'K1' THEN LEFT(tp.period_code, 4) || 'M03'  -- Q1 -> Mar
        WHEN RIGHT(tp.period_code, 2) = 'K2' THEN LEFT(tp.period_code, 4) || 'M06'  -- Q2 -> Jun
        WHEN RIGHT(tp.period_code, 2) = 'K3' THEN LEFT(tp.period_code, 4) || 'M09'  -- Q3 -> Sep
        WHEN RIGHT(tp.period_code, 2) = 'K4' THEN LEFT(tp.period_code, 4) || 'M12'  -- Q4 -> Dec
    END AS year_month,
    gdp.value_nok_million,
    ROUND(
        ((gdp.value_nok_million - LAG(gdp.value_nok_million, 4) OVER (ORDER BY tp.period_code)) 
         / LAG(gdp.value_nok_million, 4) OVER (ORDER BY tp.period_code)) * 100, 
        2
    ) AS yoy_change_percent
FROM {{ source('raw_ssb', 'ssb_09190_gdp_quarterly') }} gdp
JOIN time_periods tp ON gdp.period_id = tp.period_id
WHERE gdp.gdp_type = 'mainland'
ORDER BY tp.period_code
