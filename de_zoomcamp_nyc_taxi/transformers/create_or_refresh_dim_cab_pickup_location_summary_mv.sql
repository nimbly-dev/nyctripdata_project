DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_matviews WHERE schemaname = 'dim' AND matviewname = 'dim_cab_pickup_location_summary_mv ') THEN
        REFRESH MATERIALIZED VIEW dim.dim_cab_pickup_location_summary_mv;
    ELSE
        CREATE MATERIALIZED VIEW dim.dim_cab_pickup_location_summary_mv AS
        SELECT 
            ft.pu_location_id,                
            dzm.borough AS borough_pickup, 
            dzm.zone AS zone_pickup,
            COUNT(ft.dwid) AS total_trips,
            ROUND(AVG(ft.fare_amount)::numeric, 2) AS avg_fare,
            ROUND(AVG(ft.trip_distance)::numeric, 2) AS avg_distance
        FROM 
            fact.fact_tripdata ft
        LEFT JOIN 
            dim.dim_zone_mapping dzm
        ON 
            ft.pu_location_id = dzm.location_id
        GROUP BY 
            ft.pu_location_id, 
            dzm.borough, 
            dzm.zone;
    END IF;
END $$;
