WITH unique_data AS (
    SELECT DISTINCT ON (dwid, pickup_datetime, dropoff_datetime)
        dwid,
        cab_type,
        fare_amount,
        total_amount,
        trip_distance,
        ratecode_id,
        vendor_id,
        pu_location_id,
        do_location_id,
        pickup_datetime,
        dropoff_datetime,
        payment_type,
        dispatching_base_num,
        affiliated_base_number
    FROM {{ df_1 }}
    ORDER BY dwid, pickup_datetime, dropoff_datetime
)
INSERT INTO fact.fact_tripdata_{{ year_month }} (
    dwid,
    cab_type,
    fare_amount,
    total_amount,
    trip_distance,
    ratecode_id,
    vendor_id,
    pu_location_id,
    do_location_id,
    pickup_datetime,
    dropoff_datetime,
    payment_type,
    dispatching_base_num,
    affiliated_base_number
)
SELECT 
    dwid,
    cab_type,
    fare_amount,
    total_amount,
    trip_distance,
    ratecode_id,
    vendor_id,
    pu_location_id,
    do_location_id,
    pickup_datetime,
    dropoff_datetime,
    payment_type,
    dispatching_base_num,
    affiliated_base_number
FROM unique_data 
ON CONFLICT (dwid, pickup_datetime, dropoff_datetime) 
DO UPDATE SET
    cab_type = EXCLUDED.cab_type,
    fare_amount = EXCLUDED.fare_amount,
    total_amount = EXCLUDED.total_amount,
    trip_distance = EXCLUDED.trip_distance,
    ratecode_id = EXCLUDED.ratecode_id,
    vendor_id = EXCLUDED.vendor_id,
    pu_location_id = EXCLUDED.pu_location_id,
    do_location_id = EXCLUDED.do_location_id,
    payment_type = EXCLUDED.payment_type,
    dispatching_base_num = EXCLUDED.dispatching_base_num,
    affiliated_base_number = EXCLUDED.affiliated_base_number;