from pyspark.sql.functions import col, lit
import os
from datetime import datetime
from de_zoomcamp_nyc_taxi.utils.spark.spark_util import get_spark_session, get_dataframe_schema
from de_zoomcamp_nyc_taxi.utils.common.common_util import validate_parquet_files

SPARK_LAKEHOUSE_FILES_DIR = os.getenv(
    'SPARK_LAKEHOUSE_DIR_FILES',
    '/opt/spark/spark-lakehouse/partitioned'
)

@data_exporter
def export_data(data, *args, **kwargs):
    LOG = kwargs.get('logger')
    year_month = kwargs['year_month']  # Expected format: '2023_10'
    pipeline_run_name = kwargs['pipeline_run_name']
    spark_mode = kwargs['spark_mode']
    tripdata_type = kwargs['tripdata_type']

    if not all([year_month, tripdata_type]):
        raise ValueError("Error: 'year_month' and 'tripdata_type' must be provided.")

    LOG.info(f"Initializing Spark session for pipeline run: {pipeline_run_name} in {spark_mode} mode...")

    additional_configs = {
        'spark.jars': '/opt/spark/third-party-jars/postgresql-42.2.24.jar',
        'spark.driver.extraClassPath': '/opt/spark/third-party-jars/postgresql-42.2.24.jar'
    }

    appname = f'{pipeline_run_name}_export_dev_pq_{tripdata_type}_data_to_stage_psql'
    spark = get_spark_session(
        mode=spark_mode,
        additional_configs=additional_configs,
        appname=appname
    )

    base_read_path = os.path.join(
        SPARK_LAKEHOUSE_FILES_DIR,
        f'{tripdata_type}/tmp/pq/pre_lakehouse/{pipeline_run_name}'
    )
    base_path = os.path.join(SPARK_LAKEHOUSE_FILES_DIR, f'{tripdata_type}/data')

    year, month = map(int, year_month.split('_'))
    LOG.info(f"Processing data for {year}-{month:02d}...")

    partition_path = os.path.join(base_read_path, f'year={year}', f'month={month}')
    LOG.info(f"Checking partition path: {partition_path}")

    if not validate_parquet_files(partition_path):
        LOG.error(f"No valid Parquet files found in directory: {partition_path}")
    else:
        LOG.info("Reading data from the partition path...")
        df = spark.read.schema(get_dataframe_schema(spark, partition_path)).parquet(partition_path)
        df.cache()
        LOG.info(f"Dataframe for {year}-{month:02d} loaded.")

        num_days = (datetime(year, month, 1).replace(month=month % 12 + 1) - datetime(year, month, 1)).days
        for day in range(1, num_days + 1):
            date_str = f'{year}-{month:02d}-{day:02d}'
            LOG.info(f"Processing data for {date_str}...")

            start_datetime = f'{date_str} 00:00:00'
            end_datetime = f'{date_str} 23:59:59'

            df_filtered = df.filter(
                (col('pickup_datetime') >= lit(start_datetime)) &
                (col('pickup_datetime') <= lit(end_datetime))
            )

            if df_filtered.head(1):
                LOG.info(f"Data found for {date_str}. Proceeding with writing...")
                df_single_file = df_filtered.coalesce(1)
                write_path = os.path.join(base_path, f'partition-date={date_str}')
                LOG.info(f"Writing data to {write_path}...")

                df_single_file.write.mode("overwrite").parquet(write_path)

                files = [f for f in os.listdir(write_path) if f.endswith('.parquet')]
                if files:
                    os.rename(
                        os.path.join(write_path, files[0]),
                        os.path.join(write_path, 'data.parquet')
                    )
                    for f in files[1:]:
                        os.remove(os.path.join(write_path, f))
                else:
                    LOG.warning(f"No Parquet files found in {write_path} after writing.")

                for file_name in os.listdir(write_path):
                    file_path = os.path.join(write_path, file_name)
                    if file_name.endswith('.crc'):
                        os.remove(file_path)
            else:
                LOG.info(f"No data found for {date_str}. Skipping...")

    LOG.info("Data export completed. Stopping Spark session.")
    spark.stop()
