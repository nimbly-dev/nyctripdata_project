if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer

import os
from os import path
from mage_ai.data_preparation.repo_manager import RepoConfig, get_repo_path
from mage_ai.services.spark.config import SparkConfig
from mage_ai.services.spark.spark import get_spark_session
from pyspark.sql.functions import col, year as pyspark_year, month as pyspark_month, when, lit
from de_zoomcamp_nyc_taxi.model.schema.green_tripdata import GreenTripDataSchema
from de_zoomcamp_nyc_taxi.utils.spark.spark_util import get_spark_session, get_dataframe_schema, cache_and_delete_files
from de_zoomcamp_nyc_taxi.utils.common.common_util import validate_parquet_files
from functools import reduce
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
import calendar
from datetime import datetime


SPARK_LAKEHOUSE_FILES_DIR = os.getenv('SPARK_LAKEHOUSE_DIR_FILES', '/opt/spark/spark-lakehouse/partitioned')
SPARK_LAKEHOUSE_DIR = os.getenv('SPARK_LAKEHOUSE_DIR', '/opt/spark/spark-lakehouse')

@transformer
def transform(data, *args, **kwargs):
    """
    Build Spark Data Cleaning Logical Plan
    """
    start_year = kwargs['start_year']
    start_month = kwargs['start_month']
    end_year = kwargs['end_year']
    end_month = kwargs['end_month']
    pipeline_run_name = kwargs['pipeline_run_name']
    tripdata_type = kwargs['tripdata_type']
    spark_mode = kwargs['spark_mode']
    LOG = kwargs.get('logger')
    
    if not start_year or not start_month or not end_year or not end_month:
        raise ValueError("Error: 'start_year', 'start_month', 'end_year', and 'end_month' must be provided.")
    
    pq_dir = kwargs['configuration'].get('pq_dir')
    base_stage_path = f'{SPARK_LAKEHOUSE_DIR}/partitioned/{tripdata_type}/tmp/pq/{pq_dir}/{pipeline_run_name}'

    appname=f'{pipeline_run_name}_spark_clean_green_taxi_data'
    spark = get_spark_session(mode=spark_mode, appname=appname)
    LOG.info("Spark session initiated.")

    green_tripdata_schema = GreenTripDataSchema(spark_session=spark)

    # Loop over the date range
    start_date = datetime(start_year, start_month, 1)
    last_day = calendar.monthrange(end_year, end_month)[1]
    end_date = datetime(end_year, end_month, last_day)
    
    current_date = start_date
    while current_date <= end_date:
        year = current_date.year
        month = current_date.month

        LOG.info(f"Processing data for {year}-{month}")

        partition_path = os.path.join(base_stage_path, f'year={year}', f'month={month}')
        LOG.info(f'Partition path: {partition_path}')

        if not validate_parquet_files(partition_path):
            raise FileNotFoundError(f"No valid Parquet files found in directory: {partition_path}")

        df = spark.read.schema(get_dataframe_schema(spark,partition_path)).parquet(partition_path)
        df = cache_and_delete_files(df)
        # Apply transformations
        df = (
            df.dropDuplicates()  
            .dropna(subset=["pu_location_id", "do_location_id"])
            .filter((col('trip_distance') > 0) & (col('trip_distance') <= 10000))
            .filter((col('fare_amount') > 0) & (col('fare_amount') <= 100000))
            .filter(col('passenger_count') > 0)
            .filter(col('passenger_count').isNotNull())
            .filter(col('pickup_datetime') != col('dropoff_datetime'))
            .withColumn('store_and_fwd_flag', col('store_and_fwd_flag').substr(0, 1))
        )

        df.write.mode("overwrite").parquet(partition_path)
        
        LOG.info(f"Writing cleaned data to Parquet files at: {base_stage_path}")

        current_date = datetime(year + (month // 12), (month % 12) + 1, 1)

    LOG.info("Transformation complete")
    spark.stop()

def read_parquet(args, kwargs) -> DataFrame:
    """Helper function to read parquet files based on the year and month."""
    start_year = kwargs['start_year']
    start_month = kwargs['start_month']
    end_year = kwargs['end_year']
    end_month = kwargs['end_month']
    pipeline_run_name = kwargs['pipeline_run_name']
    tripdata_type = kwargs['tripdata_type']
    pq_dir = kwargs['configuration'].get('pq_dir')
    spark_mode = kwargs['spark_mode']

    base_stage_path = f'{SPARK_LAKEHOUSE_DIR}/partitioned/{tripdata_type}/tmp/pq/{pq_dir}/{pipeline_run_name}'

    spark = get_spark_session(mode=spark_mode, appname='test_all_conditions_spark_clean_green_taxi_data')
    
    dfs = []
    # Loop over the date range
    start_date = datetime(start_year, start_month, 1)
    end_date = datetime(end_year, end_month, 1)
    current_date = start_date
    
    while current_date <= end_date:
        year = current_date.year
        month = current_date.month
        partition_path = os.path.join(base_stage_path, f'year={year}', f'month={month}')
        
        if os.path.exists(partition_path):
            df = spark.read.schema(get_dataframe_schema(spark,partition_path)).parquet(partition_path)
            dfs.append(df)
        
        current_date = datetime(year + (month // 12), (month % 12) + 1, 1)
    
    if not dfs:
        raise ValueError("No parquet files found in the specified date range.")
    
    return reduce(DataFrame.unionAll, dfs)

@test
def test_all_conditions(*args, **kwargs) -> None:
    """
    Test multiple conditions on the parquet data.
    """
    df = read_parquet(args, kwargs)
    assert df.count() == df.dropDuplicates().count(), "The output DataFrame contains duplicate rows"

    assert df.filter((col('trip_distance') <= 0) | (col('trip_distance') > 10000)).count() == 0, "Unreasonable trip distances"
    assert df.filter((col('fare_amount') <= 0) | (col('fare_amount') > 100000)).count() == 0, "Unreasonable fare amounts"
    assert df.filter((col('trip_distance') == 0) | (col('passenger_count') == 0)).count() == 0, "Zero trip_distance or passenger_count"
    assert df.filter(col('pickup_datetime') == col('dropoff_datetime')).count() == 0, "Same pickup and dropoff times"
    assert df.filter(~col('store_and_fwd_flag').isin(['Y', 'N'])).count() == 0, "Invalid store_and_fwd_flag"
    assert df.filter(F.col("pu_location_id").isNull()).count() == 0, "Null values found in pu_location_id"
    assert df.filter(F.col("do_location_id").isNull()).count() == 0, "Null values found in do_location_id"
