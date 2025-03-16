{{ config(
    materialized='table',
    unique_key='do_location_id',
    schema='production_dim'
) }}

SELECT 
    ft.do_location_id,                
    dzm.borough AS borough_dropoff, 
    dzm.zone AS zone_dropoff,
    dzm.latitude AS latitude_dropoff,
    dzm.longitude AS longitude_dropoff,
    dzm.geolocation AS geolocation_dropoff,
    COALESCE(COUNT(ft.dwid), 0) AS total_trips,
    COALESCE(ROUND(AVG(ft.fare_amount)::numeric, 2), 0) AS avg_fare,
    COALESCE(ROUND(AVG(ft.trip_distance)::numeric, 2), 0) AS avg_distance
FROM {{ ref('fact_tripdata') }} ft
LEFT JOIN {{ ref('dim_zone_mapping') }} dzm
    ON ft.do_location_id = dzm.location_id
GROUP BY 
    ft.do_location_id, 
    dzm.borough,
    dzm.latitude,
    dzm.longitude,
    dzm.geolocation, 
    dzm.zone
