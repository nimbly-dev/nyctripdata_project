{{ config(
    materialized='table',
    unique_key='pu_location_id',
    schema='production_dim'
) }}

SELECT 
    ft.pu_location_id,                
    dzm.borough AS borough_pickup, 
    dzm.zone AS zone_pickup,
    dzm.latitude AS latitude_pickup,
    dzm.longitude AS longitude_pickup,
    dzm.geolocation AS geolocation_pickup,
    COALESCE(COUNT(ft.dwid), 0) AS total_trips,
    COALESCE(ROUND(AVG(ft.fare_amount)::numeric, 2), 0) AS avg_fare,
    COALESCE(ROUND(AVG(ft.trip_distance)::numeric, 2), 0) AS avg_distance
FROM {{ ref('fact_tripdata') }} ft
LEFT JOIN {{ ref('dim_zone_mapping') }} dzm
    ON ft.pu_location_id = dzm.location_id
GROUP BY 
    ft.pu_location_id, 
    dzm.borough, 
    dzm.zone,
    dzm.latitude,
    dzm.longitude,
    dzm.geolocation
