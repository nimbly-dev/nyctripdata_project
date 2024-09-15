import math
from pyspark.sql import DataFrame
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
import os
from os import path
import concurrent.futures
import glob
import psycopg2
from typing import Any


def get_service_account(psql_environment: str):
    env_mapping = {
        'dev': ('DEV_SERVICE_ACCOUNT_USER', 'DEV_SERVICE_ACCOUNT_PASSWORD'),
        'staging': ('STAGE_SERVICE_ACCOUNT_USER', 'STAGE_SERVICE_ACCOUNT_PASSWORD'),
        'production': ('PRODUCTION_SERVICE_ACCOUNT_USER', 'PRODUCTION_SERVICE_ACCOUNT_PASSWORD'),
    }

    if psql_environment in env_mapping:
        service_account_name, service_account_password = env_mapping[psql_environment]
        return {
            'service_account_name': os.getenv(service_account_name, ''),
            'service_account_password': os.getenv(service_account_password, '')
        }

    return None


def copy_csv_files_to_postgres(connection_string: str, partition_table_name: str, 
                               csv_dir: str, logger: Any) -> None:
    """
    Combine CSV files in csv_dir and copy data to PostgreSQL using the COPY command.

    Args:
        connection_string (str): Connection string for PostgreSQL.
        partition_table_name (str): Name of the target table in PostgreSQL.
        csv_dir (str): Directory containing the CSV files to be loaded.
        logger (Any): Logger instance for logging information.
    """
    # Find all CSV files in the directory
    csv_files = glob.glob(os.path.join(csv_dir, '*.csv'))
    if not csv_files:
        raise FileNotFoundError(f"No CSV files found in {csv_dir}")

    logger.info(f"Found {len(csv_files)} CSV files in {csv_dir}. Combining into one file...")

    # Combine CSV files into one
    combined_csv_path = os.path.join(csv_dir, 'combined.csv')
    with open(combined_csv_path, 'w', encoding='utf-8') as outfile:
        for i, fname in enumerate(csv_files):
            with open(fname, 'r', encoding='utf-8') as infile:
                if i == 0:
                    # Write the header for the first file
                    header = infile.readline()
                    outfile.write(header)
                    outfile.write(infile.read())
                else:
                    # Skip the header line for subsequent files
                    infile.readline()  # Skip header
                    outfile.write(infile.read())

    logger.info(f"Combined CSV file created at {combined_csv_path}")

    # Copy combined CSV to PostgreSQL
    copy_command = f"COPY {partition_table_name} FROM STDIN WITH CSV HEADER"

    try:
        with psycopg2.connect(connection_string) as conn:
            with conn.cursor() as cur:
                logger.info(f"Copying data into PostgreSQL table {partition_table_name}...")
                with open(combined_csv_path, 'r', encoding='utf-8') as f:
                    cur.copy_expert(copy_command, f)
                conn.commit()
                logger.info("Data copy completed successfully.")
    except Exception as e:
        logger.error(f"Error copying data to PostgreSQL: {e}")
        raise
    finally:
        # Clean up combined CSV file
        if os.path.exists(combined_csv_path):
            os.remove(combined_csv_path)
            logger.info(f"Combined CSV file {combined_csv_path} has been deleted.")


def copy_csv_file_to_postgres(csv_file: str, connection_string: str, 
                              partition_table_name: str, logger: Any) -> None:
    """
    Copy a single CSV file to PostgreSQL using the COPY command.

    Args:
        csv_file (str): Path to the CSV file.
        connection_string (str): Connection string for PostgreSQL.
        partition_table_name (str): Name of the partition table.
        logger: Logger instance for logging information.
    """
    try:
        with psycopg2.connect(connection_string) as conn:
            # Each connection operates in its own transaction
            with conn.cursor() as cur:
                logger.info(f"Copying data from file: {csv_file}")
                with open(csv_file, 'r') as f:
                    copy_command = f"COPY {partition_table_name} FROM STDIN WITH CSV HEADER"
                    cur.copy_expert(copy_command, f)
                conn.commit()
    except Exception as e:
        logger.error(f"Error processing file {csv_file}: {e}")
        raise  # Re-raise the exception to halt the process if an error occurs

def copy_csv_files_parallel(connection_string: str, partition_table_name: str, 
                            csv_dir: str, logger: Any) -> None:
    """
    Copy data from multiple CSV files to PostgreSQL in parallel.

    Args:
        connection_string (str): Connection string for PostgreSQL.
        partition_table_name (str): Name of the partition table.
        csv_dir (str): Directory containing the CSV files.
        logger: Logger instance for logging information.
    """
    csv_files = glob.glob(os.path.join(csv_dir, '*.csv'))
    if not csv_files:
        raise FileNotFoundError(f"No CSV files found in {csv_dir}")

    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = [
            executor.submit(copy_csv_file_to_postgres, csv_file, connection_string, 
                            partition_table_name, logger)
            for csv_file in csv_files
        ]
        # Wait for all futures to complete and check for exceptions
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logger.error(f"Exception occurred during parallel copy: {e}")
                raise  # Stop the process if any thread fails

def batch_write_to_postgres(df: DataFrame, jdbc_url: str, temp_table_name: str, properties: dict, batch_size: int) -> None:
    """
    Writes DataFrame to PostgreSQL in batches, optimized for parallel execution.
    
    Args:
        df (DataFrame): The Spark DataFrame to write.
        jdbc_url (str): The JDBC URL to connect to the PostgreSQL database.
        temp_table_name (str): The name of the temporary table in PostgreSQL.
        properties (dict): JDBC connection properties (e.g., user, password).
        batch_size (int): The size of each batch to write to PostgreSQL.
    """
    # Repartition DataFrame by key columns to match the partitioning strategy in PostgreSQL
    df = df.repartition("dwid", "pickup_datetime", "dropoff_datetime")

    # Calculate the total number of rows
    total_rows = df.count()

    # Calculate the optimal number of partitions based on the total rows and batch size
    num_partitions = max(1, math.ceil(total_rows / batch_size))

    # Get the current number of partitions
    current_partitions = df.rdd.getNumPartitions()

    # Only repartition if the current partitions are less than the required number
    if current_partitions < num_partitions:
        df = df.repartition(num_partitions)

    # Write the DataFrame in parallel batches to the PostgreSQL database
    df.write \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", temp_table_name) \
        .option("batchsize", batch_size) \
        .option("numPartitions", num_partitions) \
        .options(**properties)  \
        .mode("append") \
        .save()

    print(f"Data written to {temp_table_name} successfully.")

def generate_create_table_query(df: DataFrame, temp_table_name: str) -> str:
    schema = df.schema
    create_table_query = f"CREATE TEMP TABLE IF NOT EXISTS {temp_table_name} ("

    columns = []
    for field in schema.fields:
        col_name = field.name
        col_type = field.dataType
        sql_type = None
        
        # Mapping PySpark types to PostgreSQL types
        if isinstance(col_type, StringType):
            sql_type = "VARCHAR"
        elif isinstance(col_type, IntegerType):
            sql_type = "INT"
        elif isinstance(col_type, FloatType):
            sql_type = "REAL"  
        elif isinstance(col_type, DoubleType):
            sql_type = "DOUBLE PRECISION"
        elif isinstance(col_type, TimestampType):
            sql_type = "TIMESTAMP"
        else:
            raise TypeError(f"Unsupported field type: {col_type}")

        columns.append(f"{col_name} {sql_type}")
    
    # Joining all column definitions
    create_table_query += ", ".join(columns) + ");"
    
    return create_table_query