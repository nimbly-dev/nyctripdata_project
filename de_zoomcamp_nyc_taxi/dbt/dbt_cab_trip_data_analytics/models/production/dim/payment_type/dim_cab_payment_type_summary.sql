{{ config(
    materialized='table',
    schema='production_dim'
) }}

SELECT 
    pt.payment_type_description,
    COUNT(ft.dwid) AS total_trips,
    ROUND(SUM(ft.fare_amount)::NUMERIC, 2) AS total_fare,
    ROUND(AVG(ft.fare_amount)::NUMERIC, 2) AS avg_fare,
    ROUND(AVG(ft.trip_distance)::NUMERIC, 2) AS avg_distance,
    ROUND(AVG(ft.total_amount)::NUMERIC, 2) AS avg_total_amount,
    COUNT(ft.dwid) FILTER (WHERE ft.cab_type = 'yellow') AS yellow_cab_trips,
    COUNT(ft.dwid) FILTER (WHERE ft.cab_type = 'green') AS green_cab_trips
FROM {{ ref('fact_tripdata') }} ft
LEFT JOIN {{ ref('dim_payment_type') }} pt
  ON ft.payment_type = pt.payment_type_id
WHERE ft.payment_type IS NOT NULL
GROUP BY pt.payment_type_description
ORDER BY total_trips DESC
