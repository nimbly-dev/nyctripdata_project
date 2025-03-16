{% macro create_partition(schema_name, base_table_name, year_month) %}
    {{ log("Creating partition for " ~ schema_name ~ "." ~ base_table_name ~ " for month " ~ year_month, info=True) }}

    {% set sql_query %}
        DO $$ 
        DECLARE 
            partition_start DATE := TO_DATE('{{ year_month }}_01', 'YYYY_MM_DD');
            partition_end DATE := (partition_start + interval '1 month')::date;
            partition_table_name TEXT;
            index_name TEXT;
        BEGIN
            partition_table_name := format('%I_%s', '{{ base_table_name }}', to_char(partition_start, 'YYYY_MM'));
            index_name := format('idx_%I_%s', '{{ base_table_name }}', to_char(partition_start, 'YYYY_MM'));

            -- Check if the partition already exists
            IF NOT EXISTS (
                SELECT 1 FROM pg_tables 
                WHERE schemaname = '{{ schema_name }}' 
                AND tablename = partition_table_name
            ) THEN
                -- Create the partition table if it does not exist
                EXECUTE format('
                    CREATE TABLE %I.%I PARTITION OF %I.%I
                    FOR VALUES FROM (%L) TO (%L);',
                    '{{ schema_name }}', partition_table_name, 
                    '{{ schema_name }}', '{{ base_table_name }}', 
                    partition_start, partition_end
                );
                RAISE NOTICE 'Created partition %I.%I', '{{ schema_name }}', partition_table_name;
            ELSE
                RAISE NOTICE 'Partition already exists: %I.%I', '{{ schema_name }}', partition_table_name;
            END IF;

            -- Check if the index exists
            IF NOT EXISTS (
                SELECT 1 FROM pg_indexes 
                WHERE schemaname = '{{ schema_name }}' 
                AND indexname = index_name
            ) THEN
                -- Create the index if it does not exist
                EXECUTE format('
                    CREATE INDEX %I ON %I.%I (pickup_datetime);',
                    index_name, '{{ schema_name }}', partition_table_name
                );
                RAISE NOTICE 'Created index %I on %I.%I', index_name, '{{ schema_name }}', partition_table_name;
            ELSE
                RAISE NOTICE 'Index already exists: %I on %I.%I', index_name, '{{ schema_name }}', partition_table_name;
            END IF;
        END $$;
    {% endset %}

    {% if execute %}
        {% do run_query(sql_query) %}
    {% endif %}
{% endmacro %}
