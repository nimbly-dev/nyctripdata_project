-- Execute the query using dblink
SELECT * FROM dblink(
    'dbname=nyc_taxi_production_postgres user=production-service-account@de-nyctripdata-project.iam.com password=password123! host=postgres-production port=5434',
    $$
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
        pickup_datetime, 
        dropoff_datetime,
        NULL::INT AS payment_type,                  
        dispatching_base_num::VARCHAR,              
        affiliated_base_number::VARCHAR       
    FROM 
        public.fhv_cab_tripdata_production_{{ year_month }}
    $$
) AS result(
    dwid TEXT, 
    cab_type TEXT, 
    fare_amount REAL, 
    total_amount REAL, 
    trip_distance REAL, 
    ratecode_id INT, 
    vendor_id INT, 
    pu_location_id INT, 
    do_location_id INT, 
    pickup_datetime TIMESTAMP, 
    dropoff_datetime TIMESTAMP, 
    payment_type INT, 
    dispatching_base_num VARCHAR, 
    affiliated_base_number VARCHAR
)