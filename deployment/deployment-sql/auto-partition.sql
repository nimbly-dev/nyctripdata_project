CREATE OR REPLACE FUNCTION create_partition_if_not_exists(table_name TEXT, target_date DATE) RETURNS VOID AS $$
DECLARE
    partition_start DATE := date_trunc('month', target_date);
    partition_end DATE := (partition_start + interval '1 month')::date;
    partition_table_name TEXT := format('%s_%s', table_name, to_char(partition_start, 'YYYY_MM'));
BEGIN
    -- Check if the partition already exists
    IF NOT EXISTS (
        SELECT 1
        FROM pg_tables
        WHERE schemaname = 'public'
        AND tablename = partition_table_name
    ) THEN
        -- Create the partition table if it does not exist
        EXECUTE format('
            CREATE TABLE public.%I PARTITION OF public.%I
            FOR VALUES FROM (%L) TO (%L);',
            partition_table_name,
            table_name,
            partition_start,
            partition_end
        );
    END IF;
END;
$$ LANGUAGE plpgsql;