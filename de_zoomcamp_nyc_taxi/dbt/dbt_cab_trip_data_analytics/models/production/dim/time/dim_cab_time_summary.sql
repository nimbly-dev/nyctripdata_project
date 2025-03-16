{{ config(
    MATERIALIZED='TABLE',
    SCHEMA='production_dim'
) }}

WITH all_datetimes AS (
    SELECT
        pickup_datetime
    FROM {{ ref('fact_tripdata') }}
),
time_attributes AS (
    SELECT
        pickup_datetime,
        EXTRACT(YEAR FROM pickup_datetime)::INT AS year,
        EXTRACT(MONTH FROM pickup_datetime)::INT AS month,
        EXTRACT(DAY FROM pickup_datetime)::INT AS day,
        (EXTRACT(DOW FROM pickup_datetime) + 1)::INT AS day_of_week,
        TRIM(TO_CHAR(pickup_datetime, 'Day')) AS day_name,
        EXTRACT(QUARTER FROM pickup_datetime)::INT AS quarter,
        CASE
            WHEN EXTRACT(DOW FROM pickup_datetime) IN (6, 0) THEN TRUE
            ELSE FALSE
        END AS is_weekend
    FROM all_datetimes
),
year_range AS (
    SELECT
        MIN(EXTRACT(YEAR FROM pickup_datetime))::INT AS start_year,
        MAX(EXTRACT(YEAR FROM pickup_datetime))::INT AS end_year
    FROM {{ ref('fact_tripdata') }}
),
holiday_base AS (
    SELECT
        dh.holiday_name,
        yr.year,
        dh.month,
        dh.day_or_rule,
        dh.holiday_type,
        dh.nyc_observance
    FROM {{ ref('dim_holiday') }} dh
    CROSS JOIN (
        SELECT GENERATE_SERIES(
            (SELECT start_year FROM year_range),
            (SELECT end_year FROM year_range)
        ) AS year
    ) yr
),
holiday_dates AS (
    SELECT
        hb.holiday_name,
        hb.year,
        hb.month,
        CASE
            WHEN hb.day_or_rule ~ '^[0-9]+$' THEN hb.day_or_rule::INT
            WHEN EXISTS (
                SELECT 1
                FROM regexp_matches(LOWER(TRIM(hb.day_or_rule)), '^(first|second|third|fourth|last)_(sunday|monday|tuesday|wednesday|thursday|friday|saturday)$')
            ) THEN EXTRACT(DAY FROM {{ nth_weekday_of_month("hb.year", "hb.month", "hb.day_or_rule") }})
            ELSE NULL
        END AS day
    FROM holiday_base hb
),
final_holidays AS (
    SELECT
        MAKE_DATE(hd.year, hd.month, hd.day::INT) AS holiday_date,
        hd.holiday_name
    FROM holiday_dates hd
    WHERE hd.day IS NOT NULL
)

SELECT
    ta.pickup_datetime,
    ta.year,
    ta.month,
    ta.day,
    ta.day_of_week,
    ta.day_name,
    ta.quarter,
    ta.is_weekend,
    CASE
        WHEN fh.holiday_name IS NOT NULL THEN TRUE
        ELSE FALSE
    END AS holiday_flag
FROM time_attributes ta
LEFT JOIN final_holidays fh
    ON MAKE_DATE(ta.year, ta.month, ta.day) = fh.holiday_date
