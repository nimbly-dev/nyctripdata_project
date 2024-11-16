if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

import logging
import os
import shutil
from os import path
from mage_ai.data_preparation.repo_manager import RepoConfig, get_repo_path
from mage_ai.services.spark.config import SparkConfig
from de_zoomcamp_nyc_taxi.utils.spark.spark_util import get_spark_session, get_dataframe_schema, cache_and_delete_files
from de_zoomcamp_nyc_taxi.utils.common.common_util import validate_parquet_files
from functools import reduce
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import col, year as pyspark_year, month as pyspark_month
from datetime import datetime
import calendar

SPARK_LAKEHOUSE_FILES_DIR = os.getenv('SPARK_LAKEHOUSE_DIR_FILES', '/opt/spark/spark-lakehouse/partitioned')
SPARK_LAKEHOUSE_DIR = os.getenv('SPARK_LAKEHOUSE_DIR', '/opt/spark/spark-lakehouse')

@transformer
def transform(data, *args, **kwargs):
    """
    Build Spark Data Cleaning Logical Plan
    """
    year_month = kwargs['year_month']
    pipeline_run_name = kwargs['pipeline_run_name']
    tripdata_type = kwargs['tripdata_type']
    spark_mode = kwargs['spark_mode']
    LOG = kwargs.get('logger')

    pq_dir = kwargs['configuration'].get('pq_dir')
    base_path = f'{SPARK_LAKEHOUSE_DIR}/partitioned/{tripdata_type}/tmp/pq/{pq_dir}/{pipeline_run_name}'

    spark = get_spark_session(appname='spark_clean_yellow_taxi_data')
    LOG.info("Spark session initiated.")

    year, month = map(int, year_month.split('_'))
    partition_path = os.path.join(base_path, f'year={year}', f'month={month}')
    LOG.info(f'Partition path: {partition_path}')

    if not validate_parquet_files(partition_path):
        raise FileNotFoundError(f"No valid Parquet files found in directory: {partition_path}")

    df = spark.read.schema(get_dataframe_schema(spark, partition_path)).parquet(partition_path)
    df = (
        df.dropDuplicates()
        .dropna(subset=["pu_location_id", "do_location_id"])
        .filter((F.col('trip_distance') > 0) & (F.col('trip_distance') <= 10000))
        .filter((F.col('fare_amount') > 0) & (F.col('fare_amount') <= 100000))
        .filter((F.col('trip_distance') > 0) & (F.col('passenger_count') > 0))
        .filter(F.col('pickup_datetime') != F.col('dropoff_datetime'))
        .withColumn('store_and_fwd_flag', F.col('store_and_fwd_flag').substr(0, 1))
    )
    if 'year' in df.columns or 'month' in df.columns:
        df = df.drop('year', 'month')

    df.write.mode("overwrite").parquet(partition_path)
    LOG.info(f"Writing cleaned data for {year}-{month} to Parquet files at: {partition_path}")

    LOG.info("Transformation complete")
    spark.stop()

def read_parquet(args, kwargs) -> DataFrame:
    """Helper function to read parquet files based on the year and month."""
    year_month = kwargs['year_month']
    pipeline_run_name = kwargs['pipeline_run_name']
    tripdata_type = kwargs['tripdata_type']
    pq_dir = kwargs['configuration'].get('pq_dir')
    spark_mode = kwargs['spark_mode']

    base_path = f'{SPARK_LAKEHOUSE_DIR}/partitioned/{tripdata_type}/tmp/pq/{pq_dir}/{pipeline_run_name}'

    spark = get_spark_session(appname='test_all_conditions_spark_clean_yellow_taxi_data')
    
    year, month = map(int, year_month.split('_'))
    partition_path = os.path.join(base_path, f'year={year}', f'month={month}')

    if not os.path.exists(partition_path):
        raise ValueError(f"No parquet files found in {partition_path}")

    df = spark.read.schema(get_dataframe_schema(spark, partition_path)).parquet(partition_path)
    
    return df

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
