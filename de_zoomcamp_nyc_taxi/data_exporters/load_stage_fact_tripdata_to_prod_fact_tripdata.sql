-- Step 1: Partition Creation Block
DO $$
DECLARE
    partition_date DATE := TO_DATE('{{ year_month }}' || '_01', 'YYYY_MM_DD');
BEGIN
    PERFORM public.create_partition_if_not_exists(
        'fact',                -- schema name
        'fact_tripdata',       -- base table name
        partition_date         -- partition date
    );
    RAISE NOTICE 'Partition created or already exists for fact.fact_tripdata_%', '{{ year_month }}';
END $$;

-- Step 2: Data Insertion Query
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
    WHERE 
        pickup_datetime IS NOT NULL 
        AND dropoff_datetime IS NOT NULL 
        AND pu_location_id IS NOT NULL 
        AND do_location_id IS NOT NULL
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
