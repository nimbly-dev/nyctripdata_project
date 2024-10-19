DO $$
BEGIN
    IF EXISTS (
        SELECT 1 
        FROM pg_matviews 
        WHERE schemaname = 'dim' AND matviewname = 'dim_time_summary_mv'
    ) THEN
        REFRESH MATERIALIZED VIEW dim.dim_time_summary_mv;
    ELSE
        CREATE MATERIALIZED VIEW dim.dim_time_summary_mv AS
        WITH holiday_dates AS (
            SELECT 
                dh.holiday_name,
                dh.nyc_observance,
                fy.year,
                dh.month,
                
                -- Determine holiday date based on day_or_rule
                CASE 
                    -- Fixed dates, e.g., "25" for a specific day of the month
                    WHEN dh.day_or_rule ~ '^[0-9]+$' THEN 
                        make_date(fy.year, dh.month, dh.day_or_rule::INT)
                    
                    -- Dynamic weekday rules using nth_weekday_of_month function with EXISTS
                    WHEN EXISTS (
                        SELECT 1
                        FROM regexp_matches(LOWER(TRIM(dh.day_or_rule)), '^(first|second|third|fourth|last) (sunday|monday|tuesday|wednesday|thursday|friday|saturday)$')
                    ) THEN
                        nth_weekday_of_month(fy.year, dh.month, dh.day_or_rule)
                    
                    -- Default to NULL if no matching pattern
                    ELSE NULL
                END AS holiday_date
            FROM 
                dim.dim_holiday dh
            CROSS JOIN (
                SELECT DISTINCT EXTRACT(YEAR FROM pickup_datetime)::INT AS year
                FROM fact.fact_tripdata
            ) fy
        )
        
        -- Main query for the materialized view
        SELECT
            EXTRACT(YEAR FROM ft.pickup_datetime)::INT AS year,
            EXTRACT(MONTH FROM ft.pickup_datetime)::INT AS month,
            EXTRACT(DAY FROM ft.pickup_datetime)::INT AS day,
            (EXTRACT(DOW FROM ft.pickup_datetime) + 1)::INT AS day_of_week,
            
            -- Determine if the day is a weekend
            (EXTRACT(DOW FROM ft.pickup_datetime) IN (6, 7)) AS is_weekend,
            
            -- Holiday name and flag based on holiday_dates
            COALESCE(hd.holiday_name, 'None') AS holiday_name,
            (hd.holiday_name IS NOT NULL) AS holiday_flag,
            
            -- Aggregated trip data
            COUNT(*) AS total_trips,
            ROUND(SUM(ft.fare_amount)::numeric, 2) AS total_fare,
            ROUND(AVG(ft.fare_amount)::numeric, 2) AS avg_fare,
            ROUND(SUM(ft.trip_distance)::numeric, 2) AS total_distance,
            ROUND(AVG(ft.trip_distance)::numeric, 2) AS avg_distance,
            ROUND(SUM(ft.total_amount)::numeric, 2) AS total_amount,
            ROUND(AVG(ft.total_amount)::numeric, 2) AS avg_total_amount
        FROM 
            fact.fact_tripdata ft
        LEFT JOIN 
            holiday_dates hd ON ft.pickup_datetime::DATE = hd.holiday_date
        GROUP BY 
            EXTRACT(YEAR FROM ft.pickup_datetime),
            EXTRACT(MONTH FROM ft.pickup_datetime),
            EXTRACT(DAY FROM ft.pickup_datetime),
            (EXTRACT(DOW FROM ft.pickup_datetime) + 1),
            (EXTRACT(DOW FROM ft.pickup_datetime) IN (6, 7)),
            hd.holiday_name,
            (hd.holiday_name IS NOT NULL);
    END IF;
END $$;
