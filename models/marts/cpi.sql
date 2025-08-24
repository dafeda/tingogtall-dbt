{{ config(materialized='table') }}

SELECT 
    acpi.id, 
    tp.period_code AS year_month, 
    acpi.consumption_group AS indicator, 
    acpi.measure_type, 
    acpi.value
FROM ssb_05327_adjusted_cpi_by_group acpi
JOIN time_periods tp ON acpi.period_id = tp.period_id
UNION ALL
SELECT
    cpi.id,
    tp.period_code AS year_month,
    cpi.consumption_group AS indicator,
    cpi.measure_type,
    cpi.value
FROM ssb_03013_cpi_by_group cpi
JOIN time_periods tp ON cpi.period_id = tp.period_id
