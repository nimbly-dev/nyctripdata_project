CREATE TABLE fact.fact_tripdata (
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
    affiliated_base_number VARCHAR,
    -- Adding the primary key
 	PRIMARY KEY (dwid, pickup_datetime, dropoff_datetime) -- Composite primary key
) PARTITION BY RANGE(pickup_datetime)