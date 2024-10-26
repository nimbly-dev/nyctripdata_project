DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_matviews WHERE schemaname = 'dim' AND matviewname = 'dim_pickup_dropoff_heatmap_mv') THEN
        REFRESH MATERIALIZED VIEW dim.dim_pickup_dropoff_heatmap_mv;
    ELSE
        CREATE MATERIALIZED VIEW dim.dim_pickup_dropoff_heatmap_mv AS
        SELECT 
            pickup.borough AS borough_pickup,
            dropoff.borough AS borough_dropoff,
            COUNT(*) AS total_trips
        FROM 
            fact.fact_tripdata ft
        LEFT JOIN 
            dim.dim_zone_mapping pickup ON ft.pu_location_id = pickup.location_id
        LEFT JOIN 
            dim.dim_zone_mapping dropoff ON ft.do_location_id = dropoff.location_id
        WHERE 
            pickup.borough IS NOT NULL 
            AND dropoff.borough IS NOT NULL
        GROUP BY 
            pickup.borough, 
            dropoff.borough;
    END IF;
END $$;
