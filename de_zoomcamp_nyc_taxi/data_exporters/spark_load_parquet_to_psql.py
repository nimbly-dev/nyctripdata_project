if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

import os
import glob
import shutil
import time
from datetime import datetime
from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader, ConfigKey
from de_zoomcamp_nyc_taxi.utils.spark.spark_util import get_spark_session
from de_zoomcamp_nyc_taxi.utils.sql.sql_util import get_service_account, copy_csv_files_parallel, copy_csv_files_to_postgres
from mage_ai.io.postgres import Postgres
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, TimestampType


SPARK_LAKEHOUSE_FILES_DIR = os.getenv('SPARK_LAKEHOUSE_FILES_DIR', '/opt/spark/spark-lakehouse/partitioned')

@data_exporter
def export_data(data, *args, **kwargs):
    """
    Exports data from Spark to PostgreSQL using the Parallel COPY command for bulk inserts, while preserving the dynamic partitioning.
    """
    LOG = kwargs.get('logger')
    year_month = kwargs['year_month']
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

    year, month = map(int, year_month.split('_'))
    partition_path = os.path.join(base_read_path, f'year={year}', f'month={month}')
    df = spark.read.parquet(partition_path)

    df = df.drop('year', 'month')

    temp_csv_dir = os.path.join(base_csv_temp_path, f'year={year}', f'month={month}')
    df.write.csv(temp_csv_dir, header=True, mode="overwrite")

    csv_files = glob.glob(os.path.join(temp_csv_dir, '*.csv'))
    if not csv_files:
        raise FileNotFoundError(f"No CSV files found in {temp_csv_dir}")

    connection_string = f"dbname={db_name} user={service_account_name} password={service_account_password} host={host_name} port={host_port}"

    with Postgres.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
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
            temp_table_name = f'"temp"."temp_{tripdata_type}_{year}_{month:02d}"'
            create_temp_table_query = f"""
                CREATE TABLE IF NOT EXISTS {temp_table_name} (
                    LIKE public.{source_table_name} INCLUDING ALL
                );
            """
            LOG.info(f"Executing query: {create_temp_table_query}")
            loader.execute(create_temp_table_query)
            loader.commit()

            copy_csv_files_parallel(connection_string, temp_table_name, temp_csv_dir, LOG)

            upsert_query = f"""
                INSERT INTO {partition_table_name} ({', '.join(df.columns)})
                SELECT {', '.join(df.columns)}
                FROM {temp_table_name}
                WHERE {pickup_datetime} >= '{year}-{month:02d}-01'
                AND {pickup_datetime} < '{year + (month // 12)}-{(month % 12) + 1:02d}-01'
                ON CONFLICT ({primary_key}, {pickup_datetime}, {dropoff_datetime})
                DO UPDATE SET {', '.join([f'{col} = EXCLUDED.{col}' for col in df.columns if col != primary_key])};
            """
            loader.execute(upsert_query)
            loader.commit()

            loader.execute(f"DROP TABLE IF EXISTS {temp_table_name};")
            loader.commit()

    shutil.rmtree(temp_csv_dir)
    LOG.info(f"Partition for {year}-{month:02d} processed successfully.")

    LOG.info("Data load from PostgreSQL completed.")

    spark.stop()
