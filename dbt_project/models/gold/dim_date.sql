{{
    config(
        materialized='table',
        schema='gold'
    )
}}

WITH date_spine AS (
    SELECT
        toDate(concat(toString(2020 + floor(generate_series / 365)), '-01-01')) + toIntervalDay(generate_series % 365) AS full_date
    FROM generate_series(0, 365 * 15)
    WHERE full_date <= toDate('2034-12-31')
)

SELECT
    toYYYYMMDD(full_date) AS date_sk,
    full_date,
    toDayOfWeek(full_date) AS day_of_week,
    formatDateTime(full_date, '%W') AS day_name,
    toDayOfMonth(full_date) AS day_of_month,
    toDayOfYear(full_date) AS day_of_year,
    toWeek(full_date) AS week_of_year,
    toMonth(full_date) AS month_number,
    formatDateTime(full_date, '%M') AS month_name,
    toQuarter(full_date) AS quarter,
    toYear(full_date) AS year,
    toDayOfWeek(full_date) IN (6, 7) AS is_weekend,
    full_date = toLastDayOfMonth(full_date) AS is_month_end,
    full_date = toLastDayOfMonth(full_date)
     AND toMonth(full_date) IN (3, 6, 9, 12) AS is_quarter_end,
    toMonth(full_date) = 12 AND toDayOfMonth(full_date) = 31 AS is_year_end,
    toQuarter(full_date) AS fiscal_quarter,
    toYear(full_date) AS fiscal_year
FROM date_spine
ORDER BY full_date
