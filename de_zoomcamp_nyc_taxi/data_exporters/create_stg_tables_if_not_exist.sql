CREATE TABLE IF NOT EXISTS staging_fact.stg_fact_tripdata (
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
    PRIMARY KEY (dwid, pickup_datetime, dropoff_datetime)
)
PARTITION BY RANGE (pickup_datetime);

CREATE TABLE IF NOT EXISTS staging_fact.fact_tripdata (
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
    PRIMARY KEY (dwid, pickup_datetime, dropoff_datetime)
)
PARTITION BY RANGE (pickup_datetime);


CREATE TABLE IF NOT EXISTS staging.stg_yellow_cab_tripdata (
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
    PRIMARY KEY (dwid, pickup_datetime, dropoff_datetime)
)
PARTITION BY RANGE (pickup_datetime);


CREATE TABLE IF NOT EXISTS staging.stg_green_cab_tripdata (
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
    PRIMARY KEY (dwid, pickup_datetime, dropoff_datetime)
)
PARTITION BY RANGE (pickup_datetime);


CREATE TABLE IF NOT EXISTS staging.stg_fhv_cab_tripdata (
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
    PRIMARY KEY (dwid, pickup_datetime, dropoff_datetime)
)
PARTITION BY RANGE (pickup_datetime);


CREATE TABLE IF NOT EXISTS staging.combine_clean_cab_tripdata (
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
    PRIMARY KEY (dwid, pickup_datetime, dropoff_datetime)
)
PARTITION BY RANGE (pickup_datetime);


