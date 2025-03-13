# !/bin/sh
set -e

# Print environment variables
echo "ENVIRONMENT: ${ENVIRONMENT}"
echo "POSTGRES_USER: ${POSTGRES_USER}"
echo "POSTGRES_DB: ${POSTGRES_DB}"

# Verify the database environment variable is set
if [ -z "$POSTGRES_DB" ]; then
    echo "Error: POSTGRES_DB is not set."
    exit 1
fi

# Determine the schema for fact tables based on environment
if [ "$ENVIRONMENT" = "stage" ]; then
    FACT_SCHEMA="staging_fact"
elif [ "$ENVIRONMENT" = "production" ]; then
    # You can either use "production" or define a separate production fact schema
    FACT_SCHEMA="production"
else
    echo "Error: Unknown ENVIRONMENT value. Must be 'stage' or 'production'."
    exit 1
fi

echo "Creating '$FACT_SCHEMA' schema in $POSTGRES_DB..."
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname="$POSTGRES_DB" <<EOSQL
    CREATE SCHEMA IF NOT EXISTS $FACT_SCHEMA;
EOSQL
echo "'$FACT_SCHEMA' schema created successfully in $POSTGRES_DB."

echo "Creating partitioned fact_tripdata table in schema $FACT_SCHEMA..."
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname="$POSTGRES_DB" <<EOSQL
    DROP TABLE IF EXISTS $FACT_SCHEMA.fact_tripdata;
    CREATE TABLE $FACT_SCHEMA.fact_tripdata (
        dwid TEXT,
        cab_type TEXT,
        fare_amount REAL,
        total_amount REAL,
        trip_distance REAL,
        ratecode_id INT,
        vendor_id INT,
        pu_location_id INT,
        do_location_id INT,
        pickup_datetime TIMESTAMP WITHOUT TIME ZONE,
        dropoff_datetime TIMESTAMP WITHOUT TIME ZONE,
        payment_type INT,
        dispatching_base_num VARCHAR,
        affiliated_base_number VARCHAR,
        PRIMARY KEY (dwid, pickup_datetime, dropoff_datetime)
    ) PARTITION BY RANGE (pickup_datetime);
EOSQL
echo "Partitioned fact_tripdata table created successfully in schema $FACT_SCHEMA in $POSTGRES_DB."


# # Create 'temp' schema for all environments
# echo "Creating 'temp' schema in $POSTGRES_DB..."
# psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname="$POSTGRES_DB" <<EOSQL
#     CREATE SCHEMA IF NOT EXISTS temp;
# EOSQL
# echo "'temp' schema created successfully in $POSTGRES_DB."

# # Create 'dim' schema for all environments
# echo "Creating 'dim' schema in $POSTGRES_DB..."
# psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname="$POSTGRES_DB" <<EOSQL
#     CREATE SCHEMA IF NOT EXISTS dim;
# EOSQL
# echo "'dim' schema created successfully in $POSTGRES_DB."

# # Create 'fact' schema for all environments
# echo "Creating 'fact' schema in $POSTGRES_DB..."
# psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname="$POSTGRES_DB" <<EOSQL
#     CREATE SCHEMA IF NOT EXISTS fact;
# EOSQL
# echo "'fact' schema created successfully in $POSTGRES_DB."

# Create 'utility' schema for all environments
echo "Creating 'utility' schema in $POSTGRES_DB..."
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname="$POSTGRES_DB" <<EOSQL
    CREATE SCHEMA IF NOT EXISTS utility;
EOSQL
echo "'utility' schema created successfully in $POSTGRES_DB."

# Run the appropriate SQL file based on the environment
case "$ENVIRONMENT" in
    dev)
        SQL_FILE="deployment-sql/create-dev-tables.sql"
        ;;
    stage)
        SQL_FILE="deployment-sql/create-stage-tables.sql"
        ;;
    production)
        SQL_FILE="deployment-sql/create-production-tables.sql"
        ;;
    *)
        echo "Error: Invalid ENVIRONMENT value. Must be 'dev', 'stage', or 'production'."
        exit 1
        ;;
esac

if [ -f "$SQL_FILE" ]; then
    echo "Running table creation using $SQL_FILE for $ENVIRONMENT environment..."
    psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname="$POSTGRES_DB" -f "$SQL_FILE"
    echo "Tables created successfully in $POSTGRES_DB."
else
    echo "Error: $SQL_FILE not found."
    exit 1
fi

# Pass the service account username and password as variables
SERVICE_ACCOUNT_USER=${SERVICE_ACCOUNT_USER:-"service_account"}
SERVICE_ACCOUNT_PASSWORD=${SERVICE_ACCOUNT_PASSWORD:-"default_password"}

# Run create-service-users.sql for service account creation and privileges
CREATE_SERVICE_USERS_SQL="deployment-sql/create-service-users.sql"

if [ -f "$CREATE_SERVICE_USERS_SQL" ]; then
    echo "Running create-service-users.sql on $POSTGRES_DB..."

    # Substitute placeholders with actual values
    sed -e "s/{{SERVICE_ACCOUNT_USER}}/${SERVICE_ACCOUNT_USER}/g" \
        -e "s/{{SERVICE_ACCOUNT_PASSWORD}}/${SERVICE_ACCOUNT_PASSWORD}/g" \
        "$CREATE_SERVICE_USERS_SQL" | psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname="$POSTGRES_DB"

    echo "create-service-users.sql executed successfully on $POSTGRES_DB."
else
    echo "Error: create-service-users.sql not found."
fi

# Run auto-partition.sql on the specified database
echo "Running auto-partition.sql on $POSTGRES_DB..."
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname="$POSTGRES_DB" -f ./deployment-sql/auto-partition.sql
echo "auto-partition.sql executed successfully on $POSTGRES_DB."

# Set the search_path to make the function globally available
echo "Setting search_path to include the utility schema..."
PGPASSWORD="${POSTGRES_PASSWORD}" psql -h "${POSTGRES_HOST}" -p "${POSTGRES_PORT}" -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -c "ALTER DATABASE ${POSTGRES_DB} SET search_path TO utility, public;"

echo "search_path set to include utility schema for $POSTGRES_DB."

# Enable dblink extension in the public schema
echo "Enabling dblink extension in the public schema of $POSTGRES_DB..."
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname="$POSTGRES_DB" <<EOSQL
    CREATE EXTENSION IF NOT EXISTS dblink SCHEMA public;
EOSQL
echo "dblink extension enabled in the public schema of $POSTGRES_DB."
