{{ config(
    materialized='table',
    schema='production_dim'
) }}

SELECT 
    COALESCE(rc.rate_code_description, 'Unknown') AS rate_code_description,
    COUNT(ft.dwid) AS total_trips,
    ROUND(AVG(ft.fare_amount)::NUMERIC, 2) AS avg_fare,
    ROUND(AVG(ft.trip_distance)::NUMERIC, 2) AS avg_distance,
    ROUND(AVG(ft.total_amount)::NUMERIC, 2) AS avg_total_amount,
    COUNT(ft.dwid) FILTER (WHERE ft.cab_type = 'yellow') AS yellow_cab_trips,
    COUNT(ft.dwid) FILTER (WHERE ft.cab_type = 'green') AS green_cab_trips
FROM {{ ref('fact_tripdata') }} ft
LEFT JOIN {{ ref('dim_rate_code') }} rc
    ON ft.ratecode_id = rc.ratecode_id
WHERE ft.cab_type IN ('yellow', 'green')
GROUP BY 
    COALESCE(rc.rate_code_description, 'Unknown')
ORDER BY 
    total_trips DESC
