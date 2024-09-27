-- Create a central schema for shared functions if it doesn't already exist
CREATE SCHEMA IF NOT EXISTS utility;

-- Set the search path to include the utility schema
SET search_path TO utility, public;

-- Create the function in the utility schema
CREATE OR REPLACE FUNCTION utility.create_partition_if_not_exists(table_name TEXT, target_date DATE) RETURNS VOID AS $$
DECLARE
    partition_start DATE := date_trunc('month', target_date);
    partition_end DATE := (partition_start + interval '1 month')::date;
    schema_name TEXT;
    partition_table_name TEXT;
BEGIN
    -- Extract the schema and table name from the provided table_name argument
    schema_name := split_part(table_name, '.', 1);
    table_name := split_part(table_name, '.', 2);
    
    -- Construct the partition table name in the format schema.table_partition_YYYY_MM
    partition_table_name := format('%I.%s_%s', schema_name, table_name, to_char(partition_start, 'YYYY_MM'));

    -- Check if the partition already exists
    IF NOT EXISTS (
        SELECT 1
        FROM pg_tables
        WHERE schemaname = schema_name
        AND tablename = format('%s_%s', table_name, to_char(partition_start, 'YYYY_MM'))
    ) THEN
        -- Create the partition table if it does not exist
        EXECUTE format('
            CREATE TABLE %I PARTITION OF %I.%I
            FOR VALUES FROM (%L) TO (%L);',
            partition_table_name,
            schema_name,
            table_name,
            partition_start,
            partition_end
        );
    END IF;
END;
$$ LANGUAGE plpgsql;
