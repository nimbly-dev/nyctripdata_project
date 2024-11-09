from datetime import datetime
from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader, ConfigKey
from mage_ai.io.postgres import Postgres
from de_zoomcamp_nyc_taxi.utils.spark.spark_util import get_spark_session, get_dataframe_schema
from de_zoomcamp_nyc_taxi.utils.sql.sql_util import get_service_account
import os
from os import path

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

SPARK_LAKEHOUSE_FILES_DIR = os.getenv('SPARK_LAKEHOUSE_FILES_DIR', '/opt/spark/spark-lakehouse/partitioned')

@data_loader
def load_data_from_postgres(*args, **kwargs):
    LOG = kwargs.get('logger')
    year_month = kwargs['year_month']  # Expected format: '2023_10'
    pipeline_run_name = kwargs['pipeline_run_name']
    spark_mode = kwargs['spark_mode']
    tripdata_type = kwargs['tripdata_type']

    if not year_month or not tripdata_type:
        raise ValueError("Error: 'year_month' and 'tripdata_type' must be provided.")

    schema_name = 'public'
    target_dir = kwargs['configuration'].get('target_dir')
    psql_environment = kwargs['configuration'].get('psql_environment')
    source_table_name = f'{tripdata_type}_{psql_environment}'
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = kwargs['configuration'].get('config_profile')

    config = ConfigFileLoader(config_path, config_profile)
    db_name = config[ConfigKey.POSTGRES_DBNAME]
    host_name = config[ConfigKey.POSTGRES_HOST]
    host_port = config[ConfigKey.POSTGRES_PORT]
    service_account = get_service_account(psql_environment)
    service_account_name = service_account['service_account_name']
    service_account_password = service_account['service_account_password']
    jdbc_url = f'jdbc:postgresql://{host_name}:{host_port}/{db_name}'

    additional_configs = {
        'spark.jars': '/opt/spark/third-party-jars/postgresql-42.2.24.jar',
        'spark.driver.extraClassPath': '/opt/spark/third-party-jars/postgresql-42.2.24.jar'
    }

    appname = f'{pipeline_run_name}_spark_get_tripdata_from_psql'
    spark = get_spark_session(
        mode=spark_mode,
        additional_configs=additional_configs, 
        appname=appname
    )

    base_target_path = os.path.join(SPARK_LAKEHOUSE_FILES_DIR, f'{tripdata_type}/tmp/pq/{target_dir}/{pipeline_run_name}')
    year, month = map(int, year_month.split('_'))
    partition_table_name = f'{tripdata_type}_{psql_environment}_{year}_{month:02d}'

    LOG.info(f"Processing data for year={year}, month={month}...")

    min_max_query = f"""
        SELECT MIN(pickup_datetime) as min_date, MAX(pickup_datetime) as max_date
        FROM {schema_name}.{source_table_name}
        WHERE pickup_datetime >= '{year}-{month:02d}-01'
        AND pickup_datetime < '{year + (month // 12)}-{(month % 12) + 1:02d}-01'
    """

    min_max_df = spark.read.format('jdbc').options(
        url=jdbc_url,
        dbtable=f"({min_max_query}) AS min_max_dates",
        driver='org.postgresql.Driver',
        user=service_account_name,
        password=service_account_password
    ).load()

    if min_max_df.count() == 0:
        LOG.info(f"No data found for year={year}, month={month}. Skipping...")
        return

    min_value = min_max_df.first()['min_date']
    max_value = min_max_df.first()['max_date']

    LOG.info(f"Reading data from database for year={year}, month={month} with bounds {min_value} to {max_value}...")

    df = spark.read.format('jdbc').options(
        url=jdbc_url,
        dbtable=f"(SELECT * FROM {schema_name}.{partition_table_name}) AS data",
        driver='org.postgresql.Driver',
        user=service_account_name,
        password=service_account_password,
        partitionColumn="pickup_datetime",
        lowerBound=min_value.isoformat(),
        upperBound=max_value.isoformat(),
        numPartitions=8
    ).load()

    partition_path = os.path.join(base_target_path, f'year={year}', f'month={month}')
    LOG.info(f"Writing data to {partition_path}...")
    df.write.mode("overwrite").parquet(partition_path)

    LOG.info(f"Finished processing for year={year}, month={month}.")
    spark.stop()
