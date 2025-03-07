SELECT * FROM (
    SELECT * FROM {{ df_1 }}
    WHERE 
        pickup_datetime IS NOT NULL 
        AND dropoff_datetime IS NOT NULL 
        AND pu_location_id IS NOT NULL 
        AND do_location_id IS NOT NULL
) AS subquery
WHERE (SELECT COUNT(*) FROM {{ df_1 }}) > 0
UNION ALL
SELECT 
    NULL::TEXT AS dwid,
    NULL::TEXT AS cab_type,
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
WHERE (SELECT COUNT(*) FROM {{ df_1 }}) = 0
