{{ config(materialized='table') }}

SELECT 
    tp.period_code AS year_quarter,
    -- Map quarter to end-of-quarter month for consistency with economic reporting standards
    CASE 
        WHEN RIGHT(tp.period_code, 2) = 'K1' THEN LEFT(tp.period_code, 4) || 'M03'  -- Q1 -> Mar
        WHEN RIGHT(tp.period_code, 2) = 'K2' THEN LEFT(tp.period_code, 4) || 'M06'  -- Q2 -> Jun
        WHEN RIGHT(tp.period_code, 2) = 'K3' THEN LEFT(tp.period_code, 4) || 'M09'  -- Q3 -> Sep
        WHEN RIGHT(tp.period_code, 2) = 'K4' THEN LEFT(tp.period_code, 4) || 'M12'  -- Q4 -> Dec
    END AS year_month,
    gdp_type,
    gdp.value_nok_million,
    ROUND(
        ((gdp.value_nok_million - LAG(gdp.value_nok_million, 4) OVER (PARTITION BY gdp_type ORDER BY tp.period_code)) 
         / LAG(gdp.value_nok_million, 4) OVER (PARTITION BY gdp_type ORDER BY tp.period_code)) * 100, 
        2
    ) AS yoy_change_percent
FROM ssb_09190_gdp_quarterly AS gdp
JOIN time_periods tp ON gdp.period_id = tp.period_id
ORDER BY tp.period_code
