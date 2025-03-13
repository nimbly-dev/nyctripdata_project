{% macro truncate_partition(schema_name, base_table_name, year_month) -%}
    {{ log("Truncating partition for " ~ schema_name ~ "." ~ base_table_name ~ " for month " ~ year_month, info=True) }}
    {# Construct the partition table name #}
    {% set partition_table_name = base_table_name ~ '_' ~ year_month %}
    
    {# Build the SQL that checks for existence and truncates if present #}
    {% set sql_query %}
DO $$
BEGIN
    IF EXISTS (
         SELECT 1 FROM information_schema.tables 
         WHERE table_schema = '{{ schema_name }}'
           AND table_name = '{{ partition_table_name }}'
    ) THEN
         EXECUTE format('TRUNCATE TABLE %I.%I;', '{{ schema_name }}', '{{ partition_table_name }}');
    END IF;
END $$;
    {% endset %}
    
    {% if execute %}
        {% do run_query(sql_query) %}
    {% else %}
        {{ log("Not executing truncate_partition: " ~ sql_query, info=True) }}
    {% endif %}
{%- endmacro %}
