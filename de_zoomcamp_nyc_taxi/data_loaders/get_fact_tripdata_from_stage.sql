-- Use dblink to execute the SELECT statement and fetch data from the staging database
SELECT * FROM dblink(
    'dbname=nyc_taxi_staging_postgres user=staging-service-account@de-nyctripdata-project.iam.com password=password123! host=postgres-staging port=5433',
    $$
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
    FROM fact.fact_tripdata_{{ year_month }}
    $$
) AS stage_data(
    dwid TEXT,
    cab_type TEXT,
    fare_amount REAL,
    total_amount REAL,
    trip_distance REAL,
    ratecode_id INT,
    vendor_id INT,
    pu_location_id INT,
    do_location_id INT,
    pickup_datetime TIMESTAMP WITHOUT TIME ZONE,
    dropoff_datetime TIMESTAMP WITHOUT TIME ZONE,
    payment_type INT,
    dispatching_base_num VARCHAR,
    affiliated_base_number VARCHAR
)
