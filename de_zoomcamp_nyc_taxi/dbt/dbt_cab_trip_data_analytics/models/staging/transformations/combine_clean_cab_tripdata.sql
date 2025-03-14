{% set year_month = var("year_month", "2023_10") %}
{% set parts = year_month.split('_') %}
{% set year = parts[0] %}
{% set month = parts[1] %}
{% set start_date = year ~ '-' ~ month ~ '-01' %}
{% set next_month = (month | int) + 1 %}
{% set next_year = year | int %}
{% if next_month > 12 %}
  {% set next_year = next_year + 1 %}
  {% set next_month = 1 %}
{% endif %}
{% set next_month = "%02d"|format(next_month) %}
{% set end_date = next_year|string ~ '-' ~ next_month ~ '-01' %}
{% set target_schema = target.schema %}
{% set table_name = this.identifier %}

{{ 
    config(
        materialized='incremental',
        unique_key='dwid',
        partition_by={
            "field": "pickup_datetime",
            "data_type": "date",
            "granularity": "month"
        },
        on_schema_change="sync_all_columns",
        pre_hook=[
            create_partition(target_schema, table_name, year_month),
            truncate_partition(target_schema, table_name, year_month)
        ]
    )
}}



WITH yellow AS (
    SELECT 
        dwid,
        cab_type,
        fare_amount::REAL,
        total_amount::REAL,
        trip_distance::REAL,
        COALESCE(vendor_id, 0) AS vendor_id,
        COALESCE(ratecode_id, 0) AS ratecode_id,
        COALESCE(pu_location_id, 0) AS pu_location_id,
        COALESCE(do_location_id, 0) AS do_location_id,
        pickup_datetime,
        dropoff_datetime,
        COALESCE(payment_type, 0) AS payment_type,
        dispatching_base_num,
        affiliated_base_number
    FROM {{ ref('stg_yellow_cab_tripdata') }}
    WHERE pickup_datetime >= '{{ start_date }}'
      AND pickup_datetime < '{{ end_date }}'
),
green AS (
    SELECT 
        dwid,
        cab_type,
        fare_amount::REAL,
        total_amount::REAL,
        trip_distance::REAL,
        COALESCE(vendor_id, 0) AS vendor_id,
        COALESCE(ratecode_id, 0) AS ratecode_id,
        COALESCE(pu_location_id, 0) AS pu_location_id,
        COALESCE(do_location_id, 0) AS do_location_id,
        pickup_datetime,
        dropoff_datetime,
        COALESCE(payment_type, 0) AS payment_type,
        dispatching_base_num,
        affiliated_base_number
    FROM {{ ref('stg_green_cab_tripdata') }}
    WHERE pickup_datetime >= '{{ start_date }}'
      AND pickup_datetime < '{{ end_date }}'
),
fhv AS (
    SELECT 
        dwid,
        cab_type,
        CAST(NULL AS REAL) AS fare_amount,  
        CAST(NULL AS REAL) AS total_amount,
        CAST(NULL AS REAL) AS trip_distance,
        CAST(NULL AS INT) AS vendor_id,  
        CAST(NULL AS INT) AS ratecode_id, 
        COALESCE(pu_location_id, 0) AS pu_location_id,
        COALESCE(do_location_id, 0) AS do_location_id,
        pickup_datetime,
        dropoff_datetime,
        COALESCE(payment_type, 0) AS payment_type,
        COALESCE(dispatching_base_num, 'UNKNOWN') AS dispatching_base_num,
        COALESCE(affiliated_base_number, 'UNKNOWN') AS affiliated_base_number
    FROM {{ ref('stg_fhv_cab_tripdata') }}
    WHERE pickup_datetime >= '{{ start_date }}'
      AND pickup_datetime < '{{ end_date }}'
)

SELECT *
FROM yellow
UNION ALL
SELECT *
FROM green
UNION ALL
SELECT *
FROM fhv
