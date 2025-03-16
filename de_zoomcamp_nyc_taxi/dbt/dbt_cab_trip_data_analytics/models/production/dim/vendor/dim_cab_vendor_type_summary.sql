{{ config(
    materialized='table',
    schema='production_dim'
) }}


SELECT 
    dv.vendor_name,
    ft.cab_type,
    COUNT(ft.dwid) AS total_trips,
    ROUND(SUM(ft.fare_amount)::numeric, 2) AS total_fare,
    ROUND(AVG(ft.fare_amount)::numeric, 2) AS avg_fare,
    ROUND(AVG(ft.trip_distance)::numeric, 2) AS avg_distance,
    ROUND(AVG(ft.total_amount)::numeric, 2) AS avg_total_amount,
    COUNT(ft.dwid) FILTER (WHERE ft.cab_type = 'yellow') AS yellow_cab_trips,
    COUNT(ft.dwid) FILTER (WHERE ft.cab_type = 'green') AS green_cab_trips
FROM {{ ref('fact_tripdata') }} ft
LEFT JOIN {{ ref('dim_vendor') }} dv
    ON ft.vendor_id = dv.vendor_id
WHERE ft.cab_type IN ('yellow', 'green')
GROUP BY 
    dv.vendor_name, 
    ft.cab_type
ORDER BY total_trips DESC
