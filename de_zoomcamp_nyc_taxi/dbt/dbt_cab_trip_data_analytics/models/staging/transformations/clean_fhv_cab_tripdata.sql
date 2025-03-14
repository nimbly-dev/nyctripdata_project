{{ config(materialized='ephemeral') }}

WITH base AS (
    SELECT *
    FROM {{ ref('stg_fhv_cab_tripdata') }}
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
        COALESCE(pu_location_id, 0) AS pu_location_id,
        COALESCE(do_location_id, 0) AS do_location_id,
        pickup_datetime,
        dropoff_datetime,
        COALESCE(payment_type, 0) AS payment_type,
        COALESCE(dispatching_base_num, 'UNKNOWN') AS dispatching_base_num,
        COALESCE(affiliated_base_number, 'UNKNOWN') AS affiliated_base_number
    FROM filtered
)

SELECT DISTINCT *
FROM converted
