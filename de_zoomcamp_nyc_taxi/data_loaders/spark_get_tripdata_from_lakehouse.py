if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

from datetime import datetime, timedelta
from de_zoomcamp_nyc_taxi.utils.spark.spark_util import get_spark_session, get_dataframe_schema
from pyspark.sql.functions import year as spark_year, month as spark_month
import os

SPARK_LAKEHOUSE_FILES_DIR = os.getenv('SPARK_LAKEHOUSE_DIR_FILES', '/opt/spark/spark-lakehouse/partitioned')

@data_loader
def load_data(*args, **kwargs):
    LOG = kwargs.get('logger')
    year_month = kwargs['year_month']  # Expected format: '2023_10'
    pipeline_run_name = kwargs['pipeline_run_name']
    spark_mode = kwargs['spark_mode']
    tripdata_type = kwargs['tripdata_type']

    if not year_month or not tripdata_type:
        raise ValueError("Error: 'year_month' and 'tripdata_type' must be provided.")

    LOG.info(f"Initializing Spark session for pipeline run: {pipeline_run_name} in {spark_mode} mode...")

    appname = f'{pipeline_run_name}_spark_get_tripdata_from_lakehouse'
    spark = get_spark_session(
        mode=spark_mode,
        appname=appname
    )
    
    base_read_lakehouse_path = os.path.join(SPARK_LAKEHOUSE_FILES_DIR, f'{tripdata_type}/data')
    base_write_path = os.path.join(SPARK_LAKEHOUSE_FILES_DIR, f'{tripdata_type}/tmp/pq/pre_lakehouse_to_psql_production/{pipeline_run_name}')

    # Extract year and month from `year_month`
    year, month = map(int, year_month.split('_'))
    LOG.info(f"Processing data for {year}-{month:02d}...")

    # Generate file paths for each day of the specified month
    file_paths = []
    num_days = (datetime(year, month, 1).replace(month=(month % 12) + 1) - timedelta(days=1)).day
    for day in range(1, num_days + 1):
        file_path = os.path.join(base_read_lakehouse_path, f'partition-date={year}-{month:02d}-{day:02d}/data.parquet')
        if os.path.exists(file_path):
            file_paths.append(file_path)
        else:
            LOG.warning(f"File not found: {file_path}")
    
    LOG.info(f"Found {len(file_paths)} files to process.")

    # Reading data into a DataFrame
    if file_paths:
        df = spark.read.parquet(*file_paths)
        df = df.withColumn("year", spark_year("pickup_datetime")).withColumn("month", spark_month("pickup_datetime"))
        df.write.partitionBy('year', 'month').parquet(base_write_path, mode='overwrite')
        LOG.info(f'Data export completed with {df.count()} rows written.')
    else:
        LOG.warning("No valid files found for processing.")

    spark.stop()
