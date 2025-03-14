{% set year_month = var("year_month", "2023_10") %}

{% set remote_query = (
  "SELECT dwid, ''yellow'' AS cab_type, fare_amount, total_amount, trip_distance, "
  "ratecode_id, vendor_id, pu_location_id, do_location_id, pickup_datetime, dropoff_datetime, "
  "payment_type, NULL AS dispatching_base_num, NULL AS affiliated_base_number "
  "FROM public.yellow_cab_tripdata_production_" ~ year_month
) %}

{% set column_definitions = "dwid TEXT, cab_type TEXT, fare_amount REAL, total_amount REAL, "
                            "trip_distance REAL, ratecode_id INT, vendor_id INT, pu_location_id INT, "
                            "do_location_id INT, pickup_datetime TIMESTAMP, dropoff_datetime TIMESTAMP, "
                            "payment_type INT, dispatching_base_num VARCHAR, affiliated_base_number VARCHAR" %}

{% set csv_path = "/tmp/temp_copy/yellow/" ~ year_month ~ "/yellow_" ~ year_month ~ ".csv" %}
{% set copy_command = "COPY " ~ this ~ " FROM '" ~ csv_path ~ "' WITH CSV HEADER;" %}

{# Delete all rows for this month, create partition if needed, export CSV #}
{% set delete_existing = "DELETE FROM " ~ this ~ " WHERE to_char(pickup_datetime, 'YYYY_MM') = '" ~ year_month ~ "';" %}

{{
    config(
        materialized='incremental',
        unique_key='dwid',
        on_schema_change='sync_all_columns',
        pre_hook=[
            delete_existing,
            create_partition('staging', 'stg_yellow_cab_tripdata', year_month),
            export_production_data_to_csv(remote_query, 'yellow', year_month, column_definitions)
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
    'yellow' AS cab_type,
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
    NULL::VARCHAR AS dispatching_base_num,
    NULL::VARCHAR AS affiliated_base_number
FROM {{ this }}
{{ incremental_filter }}
