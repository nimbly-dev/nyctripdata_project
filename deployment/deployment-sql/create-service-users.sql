DO
$$
BEGIN
    -- Create the service account user if it does not already exist
    IF NOT EXISTS (
        SELECT 1
        FROM pg_roles
        WHERE rolname = '{{SERVICE_ACCOUNT_USER}}'
    ) THEN
        EXECUTE 'CREATE USER ' || quote_ident('{{SERVICE_ACCOUNT_USER}}') || 
                ' WITH PASSWORD ' || quote_literal('{{SERVICE_ACCOUNT_PASSWORD}}') || ';';
    END IF;
END
$$;

-- Grant privileges on existing tables in the public and temp schemas
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO "{{SERVICE_ACCOUNT_USER}}";
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA temp TO "{{SERVICE_ACCOUNT_USER}}";

-- Grant CREATE and USAGE privilege on the public and temp schemas
GRANT CREATE, USAGE ON SCHEMA public TO "{{SERVICE_ACCOUNT_USER}}";
GRANT CREATE, USAGE ON SCHEMA temp TO "{{SERVICE_ACCOUNT_USER}}";

-- Grant privileges on existing sequences in the public and temp schemas
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO "{{SERVICE_ACCOUNT_USER}}";
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA temp TO "{{SERVICE_ACCOUNT_USER}}";

-- Ensure future tables in the public and temp schemas grant SELECT, INSERT, UPDATE, DELETE privileges
ALTER DEFAULT PRIVILEGES IN SCHEMA public
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO "{{SERVICE_ACCOUNT_USER}}";

ALTER DEFAULT PRIVILEGES IN SCHEMA temp
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO "{{SERVICE_ACCOUNT_USER}}";

-- Ensure future sequences in the public and temp schemas grant USAGE and SELECT privileges
-- ALTER DEFAULT PRIVILEGES IN SCHEMA public
-- GRANT USAGE, SELECT ON SEQUENCES TO "{{SERVICE_ACCOUNT_USER}}";

-- ALTER DEFAULT PRIVILEGES IN SCHEMA temp
-- GRANT USAGE, SELECT ON SEQUENCES TO "{{SERVICE_ACCOUNT_USER}}";


-- Grant privileges on all existing schemas to the service account user
DO $$
DECLARE
    schema_name TEXT;
BEGIN
    FOR schema_name IN
        SELECT nspname
        FROM pg_namespace
        WHERE nspname NOT IN ('information_schema', 'pg_catalog', 'pg_toast')  -- Exclude system schemas
    LOOP
        EXECUTE format('GRANT USAGE, CREATE ON SCHEMA %I TO "{{SERVICE_ACCOUNT_USER}}";', schema_name);
        EXECUTE format('ALTER DEFAULT PRIVILEGES IN SCHEMA %I GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO "{{SERVICE_ACCOUNT_USER}}";', schema_name);
        EXECUTE format('ALTER DEFAULT PRIVILEGES IN SCHEMA %I GRANT USAGE, SELECT ON SEQUENCES TO "{{SERVICE_ACCOUNT_USER}}";', schema_name);
    END LOOP;
END $$;

-- -- Future-proofing for new schemas: Grant the necessary privileges automatically
-- CREATE OR REPLACE FUNCTION grant_new_schema_privileges()
-- RETURNS event_trigger
-- LANGUAGE plpgsql
-- AS $$
-- BEGIN
--     -- Grant the necessary privileges to the service account user when a new schema is created
--     EXECUTE format('GRANT USAGE, CREATE ON SCHEMA %I TO "{{SERVICE_ACCOUNT_USER}}";', tg_argv[0]);
--     EXECUTE format('ALTER DEFAULT PRIVILEGES IN SCHEMA %I GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO "{{SERVICE_ACCOUNT_USER}}";', tg_argv[0]);
--     EXECUTE format('ALTER DEFAULT PRIVILEGES IN SCHEMA %I GRANT USAGE, SELECT ON SEQUENCES TO "{{SERVICE_ACCOUNT_USER}}";', tg_argv[0]);
-- END;
-- $$;

CREATE OR REPLACE FUNCTION grant_new_schema_privileges()
RETURNS event_trigger AS $$
DECLARE
    schema_name TEXT;
BEGIN
    -- Retrieve the new schema name from the DDL commands
    SELECT object_name
      INTO schema_name
      FROM pg_event_trigger_ddl_commands()
      WHERE command_tag = 'CREATE SCHEMA'
      LIMIT 1;

    IF schema_name IS NOT NULL THEN
        EXECUTE format(
            'GRANT USAGE, CREATE ON SCHEMA %I TO "staging-service-account@de-nyctripdata-project.iam.com";',
            schema_name
        );
    END IF;
    RETURN;
END;
$$ LANGUAGE plpgsql;

CREATE EVENT TRIGGER grant_privileges_on_schema
    ON ddl_command_end
    WHEN TAG IN ('CREATE SCHEMA')
    EXECUTE FUNCTION grant_new_schema_privileges();
