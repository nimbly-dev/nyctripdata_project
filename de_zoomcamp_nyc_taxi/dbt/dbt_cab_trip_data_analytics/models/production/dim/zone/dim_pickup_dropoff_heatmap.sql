{{ config(
    materialized='table',
    unique_key=['borough_pickup', 'borough_dropoff'],
    schema='production_dim'
) }}

SELECT 
    pickup.borough AS borough_pickup,
    dropoff.borough AS borough_dropoff,
    COUNT(*) AS total_trips
FROM {{ ref('fact_tripdata') }} ft
LEFT JOIN {{ ref('dim_zone_mapping') }} pickup 
    ON ft.pu_location_id = pickup.location_id
LEFT JOIN {{ ref('dim_zone_mapping') }} dropoff 
    ON ft.do_location_id = dropoff.location_id
WHERE 
    pickup.borough IS NOT NULL 
    AND dropoff.borough IS NOT NULL
GROUP BY 
    pickup.borough, 
    dropoff.borough
