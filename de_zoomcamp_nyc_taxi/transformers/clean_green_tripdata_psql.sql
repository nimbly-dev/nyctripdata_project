-- Define a CTE to check if df_1 has any data
WITH df_1_data AS (
    SELECT * FROM {{ df_1 }}
),

data_count AS (
    SELECT COUNT(*) AS cnt FROM df_1_data
)

-- Main query
SELECT * FROM (
    -- Case when df_1 has data
    SELECT 
        CAST(dwid AS TEXT) AS dwid,
        'fhv' AS cab_type,
        NULL::REAL AS fare_amount,
        NULL::REAL AS total_amount,
        NULL::REAL AS trip_distance,
        NULL::INT AS ratecode_id,
        NULL::INT AS vendor_id,
        pu_location_id::INT,
        do_location_id::INT,
        pickup_datetime::TIMESTAMP,
        dropoff_datetime::TIMESTAMP,
        NULL::INT AS payment_type,
        dispatching_base_num::VARCHAR,
        affiliated_base_number::VARCHAR
    FROM df_1_data
    WHERE 
        pickup_datetime IS NOT NULL 
        AND dropoff_datetime IS NOT NULL 
        AND pu_location_id IS NOT NULL 
        AND do_location_id IS NOT NULL
) AS subquery
WHERE (SELECT cnt FROM data_count) > 0

UNION ALL

-- Case when df_1 is empty
SELECT 
    NULL::TEXT AS dwid,
    'fhv' AS cab_type,
    NULL::REAL AS fare_amount,
    NULL::REAL AS total_amount,
    NULL::REAL AS trip_distance,
    NULL::INT AS ratecode_id,
    NULL::INT AS vendor_id,
    NULL::INT AS pu_location_id,
    NULL::INT AS do_location_id,
    NULL::TIMESTAMP AS pickup_datetime,
    NULL::TIMESTAMP AS dropoff_datetime,
    NULL::INT AS payment_type,
    NULL::VARCHAR AS dispatching_base_num,
    NULL::VARCHAR AS affiliated_base_number
WHERE (SELECT cnt FROM data_count) = 0
