if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

import os
import glob
import shutil
import time
from calendar import monthrange
from datetime import datetime
from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader, ConfigKey
from de_zoomcamp_nyc_taxi.utils.spark.spark_util import get_spark_session
from de_zoomcamp_nyc_taxi.utils.sql.sql_util import get_service_account, copy_csv_files_parallel,copy_csv_files_to_postgres
from mage_ai.io.postgres import Postgres

SPARK_LAKEHOUSE_FILES_DIR = os.getenv('SPARK_LAKEHOUSE_FILES_DIR', '/opt/spark/spark-lakehouse/partitioned')

@data_exporter
def export_data(data, *args, **kwargs):
    """
    Exports data from Spark to PostgreSQL using the Parrelel COPY command for bulk inserts, while preserving the dynamic partitioning.
    """
    LOG = kwargs.get('logger')
    start_year = kwargs['start_year']
    start_month = kwargs['start_month']
    end_year = kwargs['end_year']
    end_month = kwargs['end_month']
    pipeline_run_name = kwargs['pipeline_run_name']
    spark_mode = kwargs['spark_mode']
    tripdata_type = kwargs['tripdata_type']
    is_overwrite_enabled = kwargs['overwrite_enabled']  

    schema_name = 'public'
    psql_environment = kwargs['configuration'].get('psql_environment')
    config_profile = kwargs['configuration'].get('config_profile')
    pq_dir = kwargs['configuration'].get('pq_dir')
    config_path = os.path.join(get_repo_path(), 'io_config.yaml')
    source_table_name = f'{tripdata_type}_{psql_environment}'

    config = ConfigFileLoader(config_path, config_profile)
    db_name = config[ConfigKey.POSTGRES_DBNAME]
    host_name = config[ConfigKey.POSTGRES_HOST]
    host_port = config[ConfigKey.POSTGRES_PORT]
    service_account = get_service_account(psql_environment)
    service_account_name = service_account['service_account_name']
    service_account_password = service_account['service_account_password']

    primary_key = 'dwid'
    pickup_datetime = 'pickup_datetime'
    dropoff_datetime = 'dropoff_datetime'

    if not all([start_year, start_month, end_year, end_month, tripdata_type]):
        raise ValueError("Error: 'start_year', 'start_month', 'end_year', 'end_month', and 'tripdata_type' must be provided.")

    LOG.info(f"Initializing Spark session for pipeline run: {pipeline_run_name} in {spark_mode} mode...")
    appname = f'{pipeline_run_name}_spark_combine_lakehouse_and_psql_data'
    additional_configs = {
        'spark.jars': '/opt/spark/third-party-jars/postgresql-42.2.24.jar',
        'spark.driver.extraClassPath': '/opt/spark/third-party-jars/postgresql-42.2.24.jar'
    }

    spark = get_spark_session(
        mode=spark_mode,
        additional_configs=additional_configs, 
        appname=appname
    )

    base_read_path = os.path.join(SPARK_LAKEHOUSE_FILES_DIR, f'{tripdata_type}/tmp/pq/{pq_dir}/{pipeline_run_name}')
    base_csv_temp_path = os.path.join(SPARK_LAKEHOUSE_FILES_DIR, f'{tripdata_type}/tmp/csv/{pipeline_run_name}')
    
    current_date = datetime(start_year, start_month, 1)
    end_date = datetime(end_year, end_month, monthrange(end_year, end_month)[1])

    connection_string = f"dbname={db_name} user={service_account_name} password={service_account_password} host={host_name} port={host_port}"

    total_start_time = time.time()
    partition_times = []

    with Postgres.with_config(ConfigFileLoader(config_path, config_profile)) as loader:

        while current_date <= end_date:
            year = current_date.year
            month = current_date.month
            next_year = year + (month // 12)
            next_month = (month % 12) + 1

            partition_start_time = time.time()

            partition_path = os.path.join(base_read_path, f'year={year}', f'month={month}')
            df = spark.read.parquet(partition_path)

            # DATAENG-17: year and month is being included on psql copy for spark_load_parquet_to_psql Pipeline
            df = df.drop('year', 'month')

            temp_csv_dir = os.path.join(base_csv_temp_path, f'year={year}', f'month={month}')
            df.write.csv(temp_csv_dir, header=True, mode="overwrite")

            csv_files = glob.glob(os.path.join(temp_csv_dir, '*.csv'))
            if not csv_files:
                raise FileNotFoundError(f"No CSV files found in {temp_csv_dir}")

            LOG.info(f"Creating partition for date: {year}-{month:02d}")
            target_date = f"{year}-{month:02d}-01"
            partition_query = f"SELECT public.create_partition_if_not_exists('public','{source_table_name}', '{target_date}');"
            loader.execute(partition_query)
            loader.commit()

            partition_table_name = f'{tripdata_type}_{psql_environment}_{year}_{month:02d}'
            
            if is_overwrite_enabled:
                loader.execute(f'TRUNCATE TABLE {partition_table_name};')
                loader.commit()


                copy_csv_files_parallel(connection_string, partition_table_name, temp_csv_dir, LOG)
            else:
                copy_command = f"COPY {temp_table_name} FROM STDIN WITH CSV HEADER"
                copy_csv_files_parallel(connection_string, temp_table_name, temp_csv_dir, LOG)

                temp_table_name = f'"temp"."temp_{tripdata_type}_{year}_{month:02d}"'
                create_temp_table_query = f"""
                    CREATE TABLE IF NOT EXISTS {temp_table_name} (
                        LIKE public.{source_table_name} INCLUDING ALL
                    );
                """
                LOG.info(f"Executing query: {create_temp_table_query}")
                loader.execute(create_temp_table_query)
                loader.commit()

                upsert_query = f"""
                    INSERT INTO {partition_table_name} ({', '.join(df.columns)})
                    SELECT {', '.join(df.columns)}
                    FROM {temp_table_name}
                    WHERE {pickup_datetime} >= '{year}-{month:02d}-01'
                    AND {pickup_datetime} < '{next_year}-{next_month:02d}-01'
                    ON CONFLICT ({primary_key}, {pickup_datetime}, {dropoff_datetime})
                    DO UPDATE SET {', '.join([f'{col} = EXCLUDED.{col}' for col in df.columns if col != primary_key])};
                """
                loader.execute(upsert_query)
                loader.commit()

                loader.execute(f"DROP TABLE IF EXISTS {temp_table_name};")

            shutil.rmtree(temp_csv_dir)

            partition_end_time = time.time()
            partition_duration = partition_end_time - partition_start_time
            partition_times.append(partition_duration)
            LOG.info(f"Partition for {year}-{month:02d} processed in {partition_duration:.2f} seconds")

            current_date = datetime(year + (month // 12), (month % 12) + 1, 1)

    total_end_time = time.time()
    total_duration = total_end_time - total_start_time
    LOG.info(f"Data load from PostgreSQL completed. Total execution time: {total_duration:.2f} seconds")

    spark.stop()
