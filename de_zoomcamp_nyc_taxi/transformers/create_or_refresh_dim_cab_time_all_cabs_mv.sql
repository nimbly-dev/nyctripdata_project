DO $$
BEGIN
    IF EXISTS (
        SELECT 1 
        FROM pg_matviews 
        WHERE schemaname = 'dim' AND matviewname = 'dim_cab_time_all_cabs_mv'
    ) THEN
        REFRESH MATERIALIZED VIEW dim.dim_cab_time_all_cabs_mv;
    ELSE
        CREATE MATERIALIZED VIEW dim.dim_cab_time_all_cabs_mv AS
        SELECT
            EXTRACT(YEAR FROM ft.pickup_datetime)::INT AS year,
            EXTRACT(MONTH FROM ft.pickup_datetime)::INT AS month,
            ft.cab_type,
            COUNT(*) AS total_trips
        FROM 
            fact.fact_tripdata ft
        GROUP BY 
            EXTRACT(YEAR FROM ft.pickup_datetime),
            EXTRACT(MONTH FROM ft.pickup_datetime),
            ft.cab_type
        ORDER BY 
            year, month, cab_type;
    END IF;
END $$;
