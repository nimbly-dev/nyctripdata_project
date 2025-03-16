{{ config(materialized='ephemeral') }}

WITH base AS (
    SELECT *
    FROM {{ ref('stg_yellow_cab_tripdata') }}
),
filtered AS (
    SELECT *
    FROM base
    WHERE pickup_datetime IS NOT NULL
      AND dropoff_datetime IS NOT NULL
      AND pu_location_id IS NOT NULL
      AND do_location_id IS NOT NULL
),
converted AS (
    SELECT
        dwid,
        cab_type,
        fare_amount,
        total_amount,
        trip_distance,
        CASE 
          WHEN TRIM(CAST(ratecode_id AS TEXT)) = '' THEN NULL
          WHEN ratecode_id::int IN (1, 2, 3, 4, 5, 6) THEN ratecode_id::int
          ELSE NULL
        END AS ratecode_id,
        CASE
          WHEN vendor_id IS NULL 
               OR TRIM(CAST(vendor_id AS TEXT)) = '' 
               OR vendor_id::int NOT IN (1, 2)
          THEN NULL
          ELSE vendor_id::int
        END AS vendor_id,
        COALESCE(pu_location_id, 0) AS pu_location_id,
        COALESCE(do_location_id, 0) AS do_location_id,
        pickup_datetime,
        dropoff_datetime,
        COALESCE(NULLIF(payment_type, ''), 5) AS payment_type,
        dispatching_base_num,
        affiliated_base_number
    FROM filtered
)

SELECT DISTINCT *
FROM converted
