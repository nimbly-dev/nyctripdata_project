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

-- Grant CREATE privilege on the public and temp schemas
GRANT CREATE ON SCHEMA public TO "{{SERVICE_ACCOUNT_USER}}";
GRANT CREATE ON SCHEMA temp TO "{{SERVICE_ACCOUNT_USER}}";

-- Grant USAGE privilege on the public and temp schemas to allow access to them
GRANT USAGE ON SCHEMA public TO "{{SERVICE_ACCOUNT_USER}}";
GRANT USAGE ON SCHEMA temp TO "{{SERVICE_ACCOUNT_USER}}";

-- Grant privileges on existing sequences in the public and temp schemas
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO "{{SERVICE_ACCOUNT_USER}}";
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA temp TO "{{SERVICE_ACCOUNT_USER}}";

-- Ensure future tables in the public and temp schemas grant SELECT, INSERT, UPDATE, DELETE privileges
ALTER DEFAULT PRIVILEGES IN SCHEMA public
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO "{{SERVICE_ACCOUNT_USER}}";

ALTER DEFAULT PRIVILEGES IN SCHEMA temp
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO "{{SERVICE_ACCOUNT_USER}}";

-- Ensure future sequences in the public and temp schemas grant USAGE and SELECT privileges
ALTER DEFAULT PRIVILEGES IN SCHEMA public
GRANT USAGE, SELECT ON SEQUENCES TO "{{SERVICE_ACCOUNT_USER}}";

ALTER DEFAULT PRIVILEGES IN SCHEMA temp
GRANT USAGE, SELECT ON SEQUENCES TO "{{SERVICE_ACCOUNT_USER}}";
