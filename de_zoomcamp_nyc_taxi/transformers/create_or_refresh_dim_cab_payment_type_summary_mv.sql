DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_matviews WHERE schemaname = 'dim' AND matviewname = 'dim_cab_payment_type_summary_mv') THEN
        REFRESH MATERIALIZED VIEW dim.dim_cab_payment_type_summary_mv;
    ELSE
        CREATE MATERIALIZED VIEW dim.dim_cab_payment_type_summary_mv AS
        SELECT 
            pt.payment_type_description,
            COUNT(ft.dwid) AS total_trips,
            ROUND(SUM(ft.fare_amount)::numeric, 2) AS total_fare,
            ROUND(AVG(ft.fare_amount)::numeric, 2) AS avg_fare,
            ROUND(AVG(ft.trip_distance)::numeric, 2) AS avg_distance,
            ROUND(AVG(ft.total_amount)::numeric, 2) AS avg_total_amount,
            COUNT(ft.dwid) FILTER (WHERE ft.cab_type = 'Yellow') AS yellow_cab_trips,
            COUNT(ft.dwid) FILTER (WHERE ft.cab_type = 'Green') AS green_cab_trips
        FROM 
            fact.fact_tripdata ft
        LEFT JOIN 
            dim.dim_payment_type pt ON ft.payment_type = pt.payment_type_id
        GROUP BY 
            pt.payment_type_description
        ORDER BY 
            total_trips DESC;
    END IF;
END $$;
