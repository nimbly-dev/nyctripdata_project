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
WHERE FALSE

UNION ALL

{% if df_1 is not none and df_1 | length > 0 %}
SELECT * FROM {{ df_1 }}
{% else %}
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
WHERE FALSE
{% endif %}

UNION ALL

{% if df_2 is not none and df_2 | length > 0 %}
SELECT * FROM {{ df_2 }}
{% else %}
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
WHERE FALSE
{% endif %}

UNION ALL

{% if df_3 is not none and df_3 | length > 0 %}
SELECT * FROM {{ df_3 }}
{% else %}
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
WHERE FALSE
{% endif %}
