{% macro export_production_data_to_csv(remote_query, cab_type, year_month, column_definitions) %}
    {% set conn_string = "dbname=" ~ env_var('PRODUCTION_POSTGRES_DBNAME')
       ~ " host=" ~ env_var('PRODUCTION_POSTGRES_HOST')
       ~ " port=" ~ env_var('PRODUCTION_POSTGRES_PORT')
       ~ " user=" ~ env_var('PRODUCTION_POSTGRES_USER')
       ~ " password=" ~ env_var('PRODUCTION_POSTGRES_PASSWORD') %}

    {{ log("Using connection string: " ~ conn_string, info=True) }}

    {% set temp_path = '/tmp/temp_copy' %}
    {% set output_dir = temp_path ~ '/' ~ cab_type ~ '/' ~ year_month %}
    {% set output_file = output_dir ~ '/' ~ cab_type ~ '_' ~ year_month ~ '.csv' %}

    {% set copy_query %}
        COPY (
            SELECT *
            FROM dblink('{{ conn_string }}', '{{ remote_query }}')
            AS t({{ column_definitions }})
        ) TO '{{ output_file }}' WITH CSV HEADER;
    {% endset %}

    {{ log("Executing export_data_to_csv with query: " ~ copy_query, info=True) }}
    {% do run_query(copy_query) %}
{% endmacro %}
