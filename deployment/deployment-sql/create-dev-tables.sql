CREATE TABLE IF NOT EXISTS public.yellow_cab_tripdata_dev (
    dwid VARCHAR NOT NULL,
    vendor_id INTEGER,
    pickup_datetime TIMESTAMP NOT NULL,
    dropoff_datetime TIMESTAMP NOT NULL,
    passenger_count INTEGER,
    trip_distance REAL,
    ratecode_id INTEGER,
    store_and_fwd_flag VARCHAR,
    pu_location_id INTEGER,
    do_location_id INTEGER,
    payment_type INTEGER,
    fare_amount REAL,
    extra REAL,
    mta_tax REAL,
    tip_amount REAL,
    tolls_amount REAL,
    improvement_surcharge REAL,
    total_amount REAL,
    congestion_surcharge REAL,
    airport_fee REAL,
    PRIMARY KEY (dwid, pickup_datetime, dropoff_datetime)  -- Composite primary key
) PARTITION BY RANGE (pickup_datetime);

CREATE TABLE IF NOT EXISTS public.green_cab_tripdata_dev (
    dwid VARCHAR NOT NULL,                      
    vendor_id INTEGER,
    pickup_datetime TIMESTAMP NOT NULL,           
    dropoff_datetime TIMESTAMP NOT NULL,
    store_and_fwd_flag VARCHAR,
    ratecode_id INTEGER,
    pu_location_id INTEGER,
    do_location_id INTEGER,
    passenger_count INTEGER,
    trip_distance REAL,
    fare_amount REAL,
    extra REAL,
    mta_tax REAL,
    tip_amount REAL,
    tolls_amount REAL,
    improvement_surcharge REAL,
    total_amount REAL,
    payment_type INTEGER,
    trip_type INTEGER,
    congestion_surcharge REAL,
    PRIMARY KEY (dwid, pickup_datetime, dropoff_datetime)
) PARTITION BY RANGE (pickup_datetime);



CREATE TABLE IF NOT EXISTS public.fhv_cab_tripdata_dev (
    dwid VARCHAR NOT NULL,      
    dispatching_base_num VARCHAR NOT NULL,                
    pickup_datetime TIMESTAMP NOT NULL,          
    dropoff_datetime TIMESTAMP NOT NULL,           
    pu_location_id INTEGER NOT NULL,                   
    do_location_id INTEGER NOT NULL,                        
    sr_flag INTEGER,                              
    affiliated_base_number VARCHAR,                
    PRIMARY KEY (dwid, pickup_datetime, dropoff_datetime)  
) PARTITION BY RANGE (pickup_datetime);

