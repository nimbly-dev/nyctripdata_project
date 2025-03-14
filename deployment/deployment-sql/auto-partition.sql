-- FUNCTION: public.create_partition_if_not_exists(schema_name, base_table_name, target_date)

-- DROP FUNCTION IF EXISTS public.create_partition_if_not_exists(text, text, date);

-- FOR PUBLIC SCHEMA
CREATE OR REPLACE FUNCTION public.create_partition_if_not_exists(
    schema_name text,
    base_table_name text,
    target_date date)
    RETURNS void
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE
    partition_start DATE := date_trunc('month', target_date);
    partition_end DATE := (partition_start + interval '1 month')::date;
    partition_table_name TEXT;
BEGIN
    -- Construct the partition table name in the format table_partition_YYYY_MM
    partition_table_name := format('%I_%s', base_table_name, to_char(partition_start, 'YYYY_MM'));

    -- Check if the partition already exists
    IF NOT EXISTS (
        SELECT 1
        FROM pg_tables
        WHERE schemaname = schema_name
        AND tablename = partition_table_name
    ) THEN
        -- Create the partition table if it does not exist
        EXECUTE format('
            CREATE TABLE %I.%I PARTITION OF %I.%I
            FOR VALUES FROM (%L) TO (%L);',
            schema_name,                  -- Schema name for the partition
            partition_table_name,         -- Name of the partition table
            schema_name,                  -- Schema of the original table
            base_table_name,              -- Base table name
            partition_start,
            partition_end
        );
        RAISE NOTICE 'Created partition %I.%I', schema_name, partition_table_name;
    ELSE
        RAISE NOTICE 'Partition already exists: %I.%I', schema_name, partition_table_name;
    END IF;
END;
$BODY$;


ALTER FUNCTION public.create_partition_if_not_exists(text, text, date)
    OWNER TO postgres;


-- FOR STAGING_FACT SCHEMA
CREATE OR REPLACE FUNCTION staging_fact.create_partition_if_not_exists(
    schema_name text,
    base_table_name text,
    target_date date)
    RETURNS void
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE
    partition_start DATE := date_trunc('month', target_date);
    partition_end DATE := (partition_start + interval '1 month')::date;
    partition_table_name TEXT;
BEGIN
    -- Construct the partition table name in the format table_partition_YYYY_MM
    partition_table_name := format('%I_%s', base_table_name, to_char(partition_start, 'YYYY_MM'));

    -- Check if the partition already exists
    IF NOT EXISTS (
        SELECT 1
        FROM pg_tables
        WHERE schemaname = schema_name
        AND tablename = partition_table_name
    ) THEN
        -- Create the partition table if it does not exist
        EXECUTE format('
            CREATE TABLE %I.%I PARTITION OF %I.%I
            FOR VALUES FROM (%L) TO (%L);',
            schema_name,                  -- Schema name for the partition
            partition_table_name,         -- Name of the partition table
            schema_name,                  -- Schema of the original table
            base_table_name,              -- Base table name
            partition_start,
            partition_end
        );
        RAISE NOTICE 'Created partition %I.%I', schema_name, partition_table_name;
    ELSE
        RAISE NOTICE 'Partition already exists: %I.%I', schema_name, partition_table_name;
    END IF;
END;
$BODY$;


ALTER FUNCTION staging_fact.create_partition_if_not_exists(text, text, date)
    OWNER TO postgres;


-- FOR PRODUCTION SCHEMA
CREATE OR REPLACE FUNCTION production.create_partition_if_not_exists(
    schema_name text,
    base_table_name text,
    target_date date)
    RETURNS void
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE
    partition_start DATE := date_trunc('month', target_date);
    partition_end DATE := (partition_start + interval '1 month')::date;
    partition_table_name TEXT;
BEGIN
    -- Construct the partition table name in the format table_partition_YYYY_MM
    partition_table_name := format('%I_%s', base_table_name, to_char(partition_start, 'YYYY_MM'));

    -- Check if the partition already exists
    IF NOT EXISTS (
        SELECT 1
        FROM pg_tables
        WHERE schemaname = schema_name
        AND tablename = partition_table_name
    ) THEN
        -- Create the partition table if it does not exist
        EXECUTE format('
            CREATE TABLE %I.%I PARTITION OF %I.%I
            FOR VALUES FROM (%L) TO (%L);',
            schema_name,                  -- Schema name for the partition
            partition_table_name,         -- Name of the partition table
            schema_name,                  -- Schema of the original table
            base_table_name,              -- Base table name
            partition_start,
            partition_end
        );
        RAISE NOTICE 'Created partition %I.%I', schema_name, partition_table_name;
    ELSE
        RAISE NOTICE 'Partition already exists: %I.%I', schema_name, partition_table_name;
    END IF;
END;
$BODY$;


ALTER FUNCTION production.create_partition_if_not_exists(text, text, date)
    OWNER TO postgres;