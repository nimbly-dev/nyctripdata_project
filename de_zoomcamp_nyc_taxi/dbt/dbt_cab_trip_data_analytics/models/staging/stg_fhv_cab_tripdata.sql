{% set year_month = var("year_month", "2023_10") %}

{% set remote_query = (
  "SELECT dwid, ''fhv'' AS cab_type, " ~
  "NULL::REAL AS fare_amount, " ~
  "NULL::REAL AS total_amount, " ~
  "NULL::REAL AS trip_distance, " ~
  "NULL::INT AS ratecode_id, " ~
  "NULL::INT AS vendor_id, " ~
  "pu_location_id::INT, " ~
  "do_location_id::INT, " ~
  "pickup_datetime, dropoff_datetime, " ~
  "NULL::INT AS payment_type, " ~
  "dispatching_base_num::VARCHAR, " ~
  "affiliated_base_number::VARCHAR " ~
  "FROM public.fhv_cab_tripdata_production_" ~ year_month
) %}
{% set column_definitions = "dwid TEXT, cab_type TEXT, fare_amount REAL, total_amount REAL, trip_distance REAL, ratecode_id INT, vendor_id INT, pu_location_id INT, do_location_id INT, pickup_datetime TIMESTAMP, dropoff_datetime TIMESTAMP, payment_type INT, dispatching_base_num VARCHAR, affiliated_base_number VARCHAR" %}

{% set csv_path = "/tmp/temp_copy/fhv/" ~ year_month ~ "/fhv_" ~ year_month ~ ".csv" %}
{% set copy_command = "COPY " ~ this ~ " FROM '" ~ csv_path ~ "' WITH CSV HEADER;" %}

{% set delete_existing = "DELETE FROM " ~ this ~ " WHERE to_char(pickup_datetime, 'YYYY_MM') = '" ~ year_month ~ "';" %}

{{ 
    config(
        materialized='incremental',
        unique_key='dwid',
        pre_hook=[
            delete_existing,
            create_partition('staging', 'stg_fhv_cab_tripdata', year_month),
            export_production_data_to_csv(remote_query, 'fhv', year_month, column_definitions)
        ],
        post_hook=copy_command
    )
}}

{% if is_incremental() %}
  {% set incremental_filter = "WHERE pickup_datetime > (SELECT MAX(pickup_datetime) FROM " ~ this ~ ")" %}
{% else %}
  {% set incremental_filter = "" %}
{% endif %}

SELECT DISTINCT ON (dwid, pickup_datetime, dropoff_datetime)
    CAST(dwid AS TEXT) AS dwid,
    'fhv' AS cab_type,
    fare_amount::REAL,
    total_amount::REAL,
    trip_distance::REAL,
    ratecode_id::INT,
    vendor_id::INT,
    pu_location_id::INT,
    do_location_id::INT,
    pickup_datetime,
    dropoff_datetime,
    payment_type::INT,
    dispatching_base_num,
    affiliated_base_number
FROM {{ this }}
{{ incremental_filter }}
