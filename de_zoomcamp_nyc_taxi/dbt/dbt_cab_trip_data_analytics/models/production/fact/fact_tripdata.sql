{% set year_month = var("year_month", "2023_10") %}
{% set target_schema = "production_fact" %}
{% set table_name = this.identifier %}
{% set csv_path = "/tmp/temp_copy/stg_fact/" ~ year_month ~ "/stg_fact_" ~ year_month ~ ".csv" %}
{% set copy_command = "COPY " ~ target_schema ~ '.' ~ table_name ~ " FROM '" ~ csv_path ~ "' WITH CSV HEADER;" %}

{{ 
    config(
        materialized='incremental',
        unique_key='dwid',
        schema=target_schema,
        partition_by={
            "field": "pickup_datetime",
            "data_type": "timestamp",   
            "granularity": "month"
        },
        pre_hook=[
            create_partition(target_schema, table_name, year_month),
            truncate_partition(target_schema, table_name, year_month)
        ],
        post_hook=copy_command
    )
}}

SELECT *
FROM {{ this }}
