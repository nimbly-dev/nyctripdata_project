{{ config(
    materialized='table',
    schema='production_dim'
) }}

SELECT
    EXTRACT(YEAR FROM ft.pickup_datetime)::INT AS year,
    EXTRACT(MONTH FROM ft.pickup_datetime)::INT AS month,
    ft.cab_type,
    COUNT(*) AS total_trips
FROM {{ ref('fact_tripdata') }} ft
GROUP BY 
    EXTRACT(YEAR FROM ft.pickup_datetime),
    EXTRACT(MONTH FROM ft.pickup_datetime),
    ft.cab_type
ORDER BY 
    year, month, cab_type
