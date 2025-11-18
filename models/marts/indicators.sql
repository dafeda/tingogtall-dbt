{{ config(materialized='table') }}

SELECT
    tp.period_type,
    tp.period_code,
    CASE
        WHEN acpi.consumption_group = 'JA_TOTAL' THEN 'CPI-AT'
        WHEN acpi.consumption_group = 'JAE_TOTAL' THEN 'CPI-ATE'
        WHEN acpi.consumption_group = 'JE_TOTAL' THEN 'CPI-AE'
        WHEN acpi.consumption_group = 'JEL_TOTAL' THEN 'CPI-AEL'
        ELSE acpi.consumption_group
    END AS indicator,
    CASE
        WHEN
            acpi.consumption_group = 'JA_TOTAL'
            THEN 'CPI adjusted for tax changes.'
        WHEN
            acpi.consumption_group = 'JAE_TOTAL'
            THEN 'CPI adjusted for tax changes and excluding energy products'
        WHEN
            acpi.consumption_group = 'JE_TOTAL'
            THEN 'CPI excluding energy products'
        WHEN
            acpi.consumption_group = 'JEL_TOTAL'
            THEN 'CPI excluding electricity'
        ELSE acpi.consumption_group
    END AS description,
    acpi.measure_type,
    acpi.value,
    tp.start_date,
    tp.end_date
FROM {{ source('raw_ssb', 'ssb_05327_adjusted_cpi_by_group') }} AS acpi
INNER JOIN
    {{ source('raw_ssb', 'time_periods') }} AS tp
    ON acpi.period_id = tp.period_id
UNION ALL
SELECT
    tp.period_type,
    tp.period_code,
    CASE
        WHEN cpi.consumption_group = 'TOTAL' THEN 'CPI'
        ELSE cpi.consumption_group
    END AS indicator,
    'Development in consumer prices for goods and services purchased by private households in Norway' AS description,
    cpi.measure_type,
    cpi.value,
    tp.start_date,
    tp.end_date
FROM {{ source('raw_ssb', 'ssb_03013_cpi_by_group') }} AS cpi
INNER JOIN
    {{ source('raw_ssb', 'time_periods') }} AS tp
    ON cpi.period_id = tp.period_id
UNION ALL
SELECT
    tp.period_type,
    tp.period_code,
    CASE
        WHEN
            gdpcap.indicator_name = 'Gross national income'
            THEN 'GNI-per-capita'
        WHEN
            gdpcap.indicator_name
            = 'MEMO: Gross national product. Constant 2015 prices'
            THEN 'GNP-constant-2015'
        WHEN
            gdpcap.indicator_name
            = 'Final consumption expenditure of households and NPISHs'
            THEN 'household-consumption'
        WHEN gdpcap.indicator_name = 'National income' THEN 'NI-per-capita'
        WHEN
            gdpcap.indicator_name = 'Gross domestic product'
            THEN 'GDP-per-capita'
    END AS indicator,
    gdpcap.indicator_name AS description,
    'index' AS measure_type,
    gdpcap.value_nok_per_capita AS value,
    tp.start_date,
    tp.end_date
FROM {{ source('raw_ssb', 'ssb_09842_gdp_per_capita') }} AS gdpcap
INNER JOIN
    {{ source('raw_ssb', 'time_periods') }} AS tp
    ON gdpcap.period_id = tp.period_id
UNION ALL
SELECT
    tp.period_type,
    tp.period_code,
    CASE
        WHEN
            gdpcap.indicator_name = 'Gross national income'
            THEN 'GNI-per-capita'
        WHEN
            gdpcap.indicator_name
            = 'MEMO: Gross national product. Constant 2015 prices'
            THEN 'GNP-constant-2015'
        WHEN
            gdpcap.indicator_name
            = 'Final consumption expenditure of households and NPISHs'
            THEN 'household-consumption'
        WHEN gdpcap.indicator_name = 'National income' THEN 'NI-per-capita'
        WHEN
            gdpcap.indicator_name = 'Gross domestic product'
            THEN 'GDP-per-capita'
    END AS indicator,
    gdpcap.indicator_name AS description,
    'yearly_change' AS measure_type,
    ROUND(
        (
            (
                gdpcap.value_nok_per_capita
                - LAG(gdpcap.value_nok_per_capita, 1)
                    OVER (
                        PARTITION BY gdpcap.indicator_name
                        ORDER BY tp.period_code
                    )
            )
            / LAG(gdpcap.value_nok_per_capita, 1)
                OVER (
                    PARTITION BY gdpcap.indicator_name ORDER BY tp.period_code
                )
        ) * 100,
        2
    ) AS value,
    tp.start_date,
    tp.end_date
FROM {{ source('raw_ssb', 'ssb_09842_gdp_per_capita') }} AS gdpcap
INNER JOIN
    {{ source('raw_ssb', 'time_periods') }} AS tp
    ON gdpcap.period_id = tp.period_id
UNION ALL
SELECT
    tp.period_type,
    tp.period_code,
    CASE
        WHEN gdp.gdp_type = 'total' THEN 'GDP-total'
        WHEN gdp.gdp_type = 'mainland' THEN 'GDP-mainland'
    END AS indicator,
    CASE
        WHEN
            gdp.gdp_type = 'total'
            THEN
                'Total GDP in constant 2022 prices, seasonally adjusted. Includes all economic activities including petroleum and international shipping.'
        WHEN
            gdp.gdp_type = 'mainland'
            THEN
                'Mainland GDP in constant 2022 prices, seasonally adjusted. Excludes petroleum activities and international shipping.'
    END AS description,
    'index' AS measure_type,
    gdp.value_nok_million AS value,
    tp.start_date,
    tp.end_date
FROM {{ source('raw_ssb', 'ssb_09190_gdp_quarterly') }} AS gdp
INNER JOIN
    {{ source('raw_ssb', 'time_periods') }} AS tp
    ON gdp.period_id = tp.period_id
UNION ALL
SELECT
    tp.period_type,
    tp.period_code,
    CASE
        WHEN gdp.gdp_type = 'total' THEN 'GDP-total'
        WHEN gdp.gdp_type = 'mainland' THEN 'GDP-mainland'
    END AS indicator,
    CASE
        WHEN
            gdp.gdp_type = 'total'
            THEN
                'Total GDP in constant 2022 prices, seasonally adjusted. Includes all economic activities including petroleum and international shipping.'
        WHEN
            gdp.gdp_type = 'mainland'
            THEN
                'Mainland GDP in constant 2022 prices, seasonally adjusted. Excludes petroleum activities and international shipping.'
    END AS description,
    'quarterly_change' AS measure_type,
    ROUND(
        (
            (
                gdp.value_nok_million
                - LAG(gdp.value_nok_million, 4)
                    OVER (PARTITION BY gdp_type ORDER BY tp.period_code)
            )
            / LAG(gdp.value_nok_million, 4)
                OVER (PARTITION BY gdp_type ORDER BY tp.period_code)
        ) * 100,
        2
    ) AS value,
    tp.start_date,
    tp.end_date
FROM {{ source('raw_ssb', 'ssb_09190_gdp_quarterly') }} AS gdp
INNER JOIN
    {{ source('raw_ssb', 'time_periods') }} AS tp
    ON gdp.period_id = tp.period_id
UNION ALL
SELECT
    tp.period_type,
    tp.period_code,
    'unemployment-rate' AS indicator,
    'Unemployment rate, Both sexes, Trend' AS description,
    'percent' AS measure_type,
    value,
    tp.start_date,
    tp.end_date
FROM {{ source('raw_ssb', 'ssb_13760_labour_force_survey') }} AS lfs
INNER JOIN
    {{ source('raw_ssb', 'time_periods') }} AS tp
    ON lfs.period_id = tp.period_id
