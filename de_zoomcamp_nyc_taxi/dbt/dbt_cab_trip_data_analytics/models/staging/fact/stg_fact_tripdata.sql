{% set year_month = var("year_month", "2022_02") %}
{% set parts = year_month.split('_') %}
{% set year = parts[0] | int %}
{% set month = parts[1] | int %}
{% set next_month = month + 1 %}
{% set next_year = year %}
{% if next_month > 12 %}
  {% set next_year = year + 1 %}
  {% set next_month = 1 %}
{% endif %}
{% set partition_start = ("%04d-%02d-01" % (year, month)) %}
{% set partition_end = ("%04d-%02d-01" % (next_year, next_month)) %}

{# Set target schema and table name explicitly #}
{% set target_schema = target.schema ~ '_fact' %}
{% set table_name = this.identifier %}

{% do log("DEBUG: target_schema is " ~ target_schema, info=True) %}
{% do log("DEBUG: table_name is " ~ table_name, info=True) %}

{# Define CSV path & COPY command #}
{% set csv_path = "/tmp/temp_copy/combined/" ~ year_month ~ "/combined_" ~ year_month ~ ".csv" %}
{% set copy_command = "COPY " ~ target_schema ~ '.' ~ table_name ~ " FROM '" ~ csv_path ~ "' WITH CSV HEADER;" %}

{# Reference the combine_clean_cab_tripdata table from staging #}
{% set combined_relation = target.schema ~ '."combine_clean_cab_tripdata"' %}

{% if is_incremental() %}
  {% set incremental_filter = "WHERE pickup_datetime > (SELECT COALESCE(MAX(pickup_datetime), '1900-01-01') FROM " ~ this ~ ")" %}
{% else %}
  {% set incremental_filter = "" %}
{% endif %}

{% set remote_query %}
WITH raw_data AS (
    SELECT 
        dwid,
        cab_type,
        fare_amount,
        total_amount,
        trip_distance,
        vendor_id,
        ratecode_id,
        pu_location_id,
        do_location_id,
        pickup_datetime,
        dropoff_datetime,
        payment_type,
        dispatching_base_num,
        affiliated_base_number
    FROM {{ combined_relation }}
)
SELECT DISTINCT ON (dwid, pickup_datetime, dropoff_datetime) *
FROM raw_data
{{ incremental_filter }}
{% endset %}

{% set column_definitions %}
dwid TEXT, cab_type TEXT, fare_amount REAL, total_amount REAL, 
trip_distance REAL, vendor_id INT, ratecode_id INT, pu_location_id INT, 
do_location_id INT, pickup_datetime TIMESTAMP, dropoff_datetime TIMESTAMP, 
payment_type INT, dispatching_base_num VARCHAR, affiliated_base_number VARCHAR
{% endset %}

{{ 
    config(
        materialized='incremental',
        unique_key='dwid',
        schema=target_schema,
        partition_by={
            "field": "pickup_datetime",
            "data_type": "date",
            "granularity": "month"
        },
        pre_hook=[
            create_partition(target_schema, table_name, year_month),
            truncate_partition(target_schema, table_name, year_month),
            export_data_to_csv(remote_query, 'combined', year_month, column_definitions)
        ],
        post_hook=copy_command
    )
}}

SELECT *
FROM {{ this }}
{% if is_incremental() %}
  WHERE pickup_datetime > (SELECT MAX(pickup_datetime) FROM {{ this }})
{% endif %}
