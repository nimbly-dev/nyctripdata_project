{% macro export_data_to_csv(query, cab_type, year_month, column_definitions) %}
    {% set temp_path = '/tmp/temp_copy' %}
    {% set output_dir = temp_path ~ '/' ~ cab_type ~ '/' ~ year_month %}
    {% set output_file = output_dir ~ '/' ~ cab_type ~ '_' ~ year_month ~ '.csv' %}
    {% set copy_query = "COPY (" ~ query | trim ~ ") TO '" ~ output_file ~ "' WITH CSV HEADER;" %}
    
    {{ log("Executing export_data_to_csv with query: " ~ copy_query, info=True) }}
    {% do run_query(copy_query) %}
{% endmacro %}
