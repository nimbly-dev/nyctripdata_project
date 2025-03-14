{{ config(materialized='ephemeral') }}

{% set year_month = var("year_month", "2023_10") %}
{% set parts = year_month.split('_') %}
{% set year = parts[0] %}
{% set month = parts[1] %}
{% set csv_path = "/tmp/temp_copy/stg_fact/" ~ year_month ~ "/stg_fact_" ~ year_month ~ ".csv" %}

{# 
  Define the query that selects data from the staging_fact.stg_fact_tripdata 
  for the given month.
#}
{% set remote_query %}
SELECT *
FROM {{ ref('stg_fact_tripdata') }}
WHERE to_char(pickup_datetime, 'YYYY_MM') = '{{ year_month }}'
{% endset %}

{# 
  Define the column definitions. Adjust these as needed to match your tables schema.
#}
{% set column_definitions = "dwid TEXT, cab_type TEXT, fare_amount REAL, total_amount REAL, trip_distance REAL, vendor_id INT, ratecode_id INT, pu_location_id INT, do_location_id INT, pickup_datetime TIMESTAMP, dropoff_datetime TIMESTAMP, payment_type INT, dispatching_base_num VARCHAR, affiliated_base_number VARCHAR" %}

{# 
  Trigger the export macro. This macro is expected to run the COPY command (or similar)
  that exports the result of remote_query to the csv_path.
#}
{{ export_data_to_csv(remote_query, 'stg_fact', year_month, column_definitions) }}

SELECT 1 AS exported
