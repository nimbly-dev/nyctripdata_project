DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_matviews WHERE schemaname = 'dim' AND matviewname = 'dim_rate_code_summary_mv') THEN
        REFRESH MATERIALIZED VIEW dim.dim_rate_code_summary_mv;
    ELSE
        CREATE MATERIALIZED VIEW dim.dim_rate_code_summary_mv AS
        SELECT 
            rc.rate_code_description,
            COUNT(ft.dwid) AS total_trips,
            ROUND(AVG(ft.fare_amount)::numeric, 2) AS avg_fare,
            ROUND(AVG(ft.trip_distance)::numeric, 2) AS avg_distance,
            ROUND(AVG(ft.total_amount)::numeric, 2) AS avg_total_amount,
            COUNT(ft.dwid) FILTER (WHERE ft.cab_type = 'yellow') AS yellow_cab_trips,
            COUNT(ft.dwid) FILTER (WHERE ft.cab_type = 'green') AS green_cab_trips
        FROM 
            fact.fact_tripdata ft
        LEFT JOIN 
            dim.dim_rate_code rc ON ft.ratecode_id = rc.ratecode_id
        WHERE 
            ft.cab_type IN ('yellow', 'green')
        GROUP BY 
            rc.rate_code_description
        ORDER BY 
            total_trips DESC;
    END IF;
END $$;
