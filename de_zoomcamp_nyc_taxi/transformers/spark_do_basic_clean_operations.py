if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

from datetime import datetime
from functools import reduce
from pyspark.sql.functions import year as spark_year, month as spark_month
from pyspark.sql.functions import col
import os
from de_zoomcamp_nyc_taxi.utils.spark.spark_util import get_spark_session, get_dataframe_schema

SPARK_LAKEHOUSE_FILES_DIR = os.getenv('SPARK_LAKEHOUSE_DIR_FILES', '/opt/spark/spark-lakehouse/partitioned')

@transformer
def transform(*args, **kwargs):
    """
    Cleans data using Spark, tailored for dynamic datasets.

    Reads Parquet files from the specified path, performs basic cleaning:
    - Removes duplicates based on a primary key (if provided).
    - Filters out rows with null values in specified columns, excluding specified columns from this check.
    Overwrites the existing Parquet files in the 'tmp/pq/{pipeline_run_name}/pre_combined_data_production' directory.

    Configuration Parameters:
    - pipeline_run_name (str): Identifier for this pipeline run.
    - spark_mode (str): Mode to initialize Spark (e.g., 'local', 'cluster').
    - tripdata_type (str): Type of trip data.
    - data_loss_threshold (float): Acceptable data loss threshold.
    - primary_key (str, optional): Column for duplicate removal.
    - columns_to_exclude_from_null_check (list of str, optional): Columns to exclude from null checks.

    Raises:
    - ValueError: If required parameters are missing.
    """
    LOG = kwargs.get('logger')
    start_year = kwargs['start_year']
    start_month = kwargs['start_month']
    end_year = kwargs['end_year']
    end_month = kwargs['end_month']
    pipeline_run_name = kwargs['pipeline_run_name']
    spark_mode = kwargs['spark_mode']
    tripdata_type = kwargs['tripdata_type']
    primary_key = kwargs['configuration'].get('primary_key')
    columns_to_exclude_from_null_check = kwargs['configuration'].get('columns_to_exclude_from_null_check', [])
    source_pq_dir = kwargs['configuration'].get('source_pq_dir')
    target_pq_dir = kwargs['configuration'].get('target_pq_dir')

    if not all([pipeline_run_name, spark_mode, tripdata_type]):
        raise ValueError("Missing required parameters: 'pipeline_run_name', 'spark_mode', 'tripdata_type'.")

    LOG.info(f"Initializing Spark session for pipeline run: {pipeline_run_name} in {spark_mode} mode...")
    
    spark = get_spark_session(mode=spark_mode, appname=f'{pipeline_run_name}_spark_do_basic_clean_operations')

    base_read_path = os.path.join(SPARK_LAKEHOUSE_FILES_DIR, f'{tripdata_type}/tmp/pq/{source_pq_dir}/{pipeline_run_name}')
    base_write_path = os.path.join(SPARK_LAKEHOUSE_FILES_DIR, f'{tripdata_type}/tmp/pq/{target_pq_dir}/{pipeline_run_name}')
    start_date = datetime(start_year, start_month, 1)
    end_date = datetime(end_year, end_month, 31)
    
    current_date = start_date
    while current_date <= end_date:
        year = current_date.year
        month = current_date.month
        LOG.info(f"Processing data for {year}-{month}")

        partition_path = os.path.join(base_read_path, f'year={year}', f'month={month}')
        LOG.info(f'Partition path: {partition_path}')

        df = spark.read.schema(get_dataframe_schema(spark,partition_path)).parquet(partition_path)
        df = df.dropDuplicates([primary_key])
        columns_to_check = [col for col in df.columns if col not in columns_to_exclude_from_null_check]
        if columns_to_check:
            df = df.filter(
                reduce(lambda a, b: a & b, (col(c).isNotNull() for c in columns_to_check))
            )   
        write_path = os.path.join(base_write_path, f'year={year}', f'month={month}')

        df.write.mode("overwrite").parquet(write_path)
        current_date = datetime(year + (month // 12), (month % 12) + 1, 1)

    LOG.info(f"Data cleaning completed. Cleaned data saved to: {base_read_path}")
    spark.stop()

@test
def test_output(*args, **kwargs) -> None:
    LOG = kwargs.get('logger')
    spark_mode = kwargs['spark_mode']
    pipeline_run_name = kwargs['pipeline_run_name']
    tripdata_type = kwargs['tripdata_type']
    columns_to_exclude_from_null_check = kwargs['configuration'].get('columns_to_exclude_from_null_check', [])
    primary_key = kwargs['configuration'].get('primary_key')
    target_pq_dir = kwargs['configuration'].get('target_pq_dir')

    spark = get_spark_session(mode=spark_mode, appname=f'test_{pipeline_run_name}_spark_do_basic_clean_operations')
    cleaned_path = os.path.join(SPARK_LAKEHOUSE_FILES_DIR, f'{tripdata_type}/tmp/pq/{target_pq_dir}/{pipeline_run_name}')
    cleaned_df = spark.read.parquet(cleaned_path)

    columns_to_check = [col for col in cleaned_df.columns if col not in columns_to_exclude_from_null_check]
    for column in columns_to_check:
        assert cleaned_df.filter(col(column).isNull()).count() == 0, f"Column '{column}' contains null values in the cleaned data."

    expected_columns = kwargs.get('expected_columns', [])
    if expected_columns:
        missing_columns = [col for col in expected_columns if col not in cleaned_df.columns]
        assert not missing_columns, f"Missing columns in cleaned data: {missing_columns}"

    if primary_key:
        key_count_df = cleaned_df.groupBy(primary_key).count()
        num_duplicates = key_count_df.filter(col("count") > 1).count()
        assert num_duplicates == 0, f"Duplicate primary keys found. Count of duplicates: {num_duplicates}"

    LOG.info(f"Data validation completed. All checks passed for: {cleaned_path}")
    spark.stop()
