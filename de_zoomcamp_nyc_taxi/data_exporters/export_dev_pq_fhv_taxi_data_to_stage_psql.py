import os
from os import path
from datetime import datetime
from pyspark.sql import SparkSession
from mage_ai.io.config import ConfigFileLoader, ConfigKey
from mage_ai.io.postgres import Postgres
from mage_ai.settings.repo import get_repo_path
from pyspark.sql.types import StructType, StringType, TimestampType, IntegerType
from de_zoomcamp_nyc_taxi.utils.spark.spark_util import get_spark_session, get_dataframe_schema

SPARK_LAKEHOUSE_FILES_DIR = os.getenv('SPARK_LAKEHOUSE_DIR_FILES', '/opt/spark/spark-lakehouse/partitioned')

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

@data_exporter
def export_data(data, *args, **kwargs):
    LOG = kwargs.get('logger')
    start_year = kwargs['start_year']
    start_month = kwargs['start_month']
    end_year = kwargs['end_year']
    end_month = kwargs['end_month']
    pipeline_run_name = kwargs['pipeline_run_name']
    tripdata_type = kwargs['tripdata_type']
    spark_mode = kwargs['spark_mode']

    schema_name = 'public'
    staging_table_name = 'fhv_tripdata_staging'
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'stage_db'

    if not start_year or not start_month or not end_year or not end_month or not tripdata_type:
        raise ValueError("Error: 'start_year', 'start_month', 'end_year', 'end_month', and 'tripdata_type' must be provided.")

    config = ConfigFileLoader(config_path, config_profile)
    stage_db_name = config[ConfigKey.POSTGRES_DBNAME]
    stage_host_name = config[ConfigKey.POSTGRES_HOST]
    stage_host_port = config[ConfigKey.POSTGRES_PORT]

    base_stage_path = os.path.join(SPARK_LAKEHOUSE_FILES_DIR, f'{tripdata_type}/tmp/pq/stage/{pipeline_run_name}')
    current_date = datetime(start_year, start_month, 1)
    end_date = datetime(end_year, end_month, 31)

    additional_configs = {
        'spark.jars': '/opt/spark/third-party-jars/postgresql-42.2.24.jar',
        'spark.driver.extraClassPath': '/opt/spark/third-party-jars/postgresql-42.2.24.jar'
    }

    appname = f'{pipeline_run_name}_export_dev_pq_fhv_taxi_data_to_stage_psql'
    spark = get_spark_session(
        mode=spark_mode,
        additional_configs=additional_configs, 
        appname=appname
    )

    stage_jdbc_url = f'jdbc:postgresql://{stage_host_name}:{stage_host_port}/{stage_db_name}'

    with Postgres.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
        temp_table_name = f'temp.{pipeline_run_name}_{tripdata_type}_temp_staging'

        LOG.info(f"Creating temporary table: {temp_table_name}")
        create_temp_table_query = f"""
            DROP TABLE IF EXISTS {temp_table_name};
            CREATE TABLE {temp_table_name} AS TABLE {staging_table_name} WITH NO DATA;
        """
        loader.execute(create_temp_table_query)
        loader.commit()

        while current_date <= end_date:
            year = current_date.year
            month = current_date.month
            partition_path = os.path.join(base_stage_path, f'year={year}', f'month={month}')

            LOG.info(f"Partition path: {partition_path}")

            if not os.path.exists(partition_path):
                raise FileNotFoundError(f"Partition path does not exist: {partition_path}")

            df = spark.read.schema(get_dataframe_schema(spark, partition_path)).parquet(partition_path)

            LOG.info("Creating partition.")
            target_date = f"{year}-{month:02d}-01"
            query = f"SELECT public.create_partition_if_not_exists('{staging_table_name}', '{target_date}');"
            loader.execute(query)
            loader.commit()

            LOG.info("Inserting data into the temporary table.")
            properties = {
                "user": "postgres",
                "password": "postgres",
                "driver": "org.postgresql.Driver"
            }

            df = df.drop("year", "month")
            df.printSchema() 
            df.write.jdbc(stage_jdbc_url, temp_table_name, mode="append", properties=properties)

            current_date = datetime(year + (month // 12), (month % 12) + 1, 1)

        LOG.info("Performing upsert from temporary table to staging table.")
        upsert_query = f"""
            INSERT INTO {staging_table_name} (
                dwid, dispatching_base_num, pickup_datetime, dropoff_datetime, pu_location_id, do_location_id, sr_flag, affiliated_base_number
            )
            SELECT 
                dwid, dispatching_base_num, pickup_datetime, dropoff_datetime, pu_location_id, do_location_id, sr_flag, affiliated_base_number
            FROM {temp_table_name}
            ON CONFLICT (dwid, pickup_datetime, dropoff_datetime)
            DO UPDATE SET
                dispatching_base_num = EXCLUDED.dispatching_base_num,
                pu_location_id = EXCLUDED.pu_location_id,
                do_location_id = EXCLUDED.do_location_id,
                sr_flag = EXCLUDED.sr_flag,
                affiliated_base_number = EXCLUDED.affiliated_base_number;
        """
        loader.execute(upsert_query)
        loader.commit()

        drop_temp_table_query = f"DROP TABLE IF EXISTS {temp_table_name};"
        LOG.info(f"Dropping temporary table: {temp_table_name}")
        loader.execute(drop_temp_table_query)
        loader.commit()

        LOG.info("Data upsert completed successfully.")

    spark.stop()
