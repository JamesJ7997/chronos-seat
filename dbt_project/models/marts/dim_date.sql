{{
    config(
        materialized='table',
        schema='gold'
    )
}}

WITH date_spine AS (
    SELECT
        CAST(dt AS DATE) AS full_date
    FROM generate_series(
        DATE '2020-01-01',
        DATE '2034-12-31',
        INTERVAL 1 DAY
    ) AS t(dt)
)

SELECT
    CAST(strftime(full_date, '%Y%m%d') AS INTEGER) AS date_sk,
    full_date,
    CAST(dayofweek(full_date) AS INTEGER) AS day_of_week,
    dayname(full_date) AS day_name,
    CAST(day(full_date) AS INTEGER) AS day_of_month,
    CAST(dayofyear(full_date) AS INTEGER) AS day_of_year,
    CAST(weekofyear(full_date) AS INTEGER) AS week_of_year,
    CAST(month(full_date) AS INTEGER) AS month_number,
    monthname(full_date) AS month_name,
    CAST(quarter(full_date) AS INTEGER) AS quarter,
    CAST(year(full_date) AS INTEGER) AS year,
    (dayofweek(full_date) IN (6, 7)) AS is_weekend,
    (full_date = last_day(full_date)) AS is_month_end,
    (full_date = last_day(full_date)
     AND month(full_date) IN (3, 6, 9, 12)) AS is_quarter_end,
    (month(full_date) = 12 AND day(full_date) = 31) AS is_year_end,
    CAST(quarter(full_date) AS INTEGER) AS fiscal_quarter,
    CAST(year(full_date) AS INTEGER) AS fiscal_year
FROM date_spine
ORDER BY full_date