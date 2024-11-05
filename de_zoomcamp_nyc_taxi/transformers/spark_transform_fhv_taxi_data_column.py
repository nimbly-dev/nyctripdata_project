if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

import logging
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year as pyspark_year, month as pyspark_month
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType, TimestampType
from de_zoomcamp_nyc_taxi.model.schema.fhv_tripdata import FHVTripDataSchema
from de_zoomcamp_nyc_taxi.utils.spark.spark_util import get_spark_session
from de_zoomcamp_nyc_taxi.utils.common.common_util import validate_parquet_files
import calendar
from datetime import datetime
from calendar import monthrange


logger = logging.getLogger(__name__)

SPARK_LAKEHOUSE_FILES_DIR = os.getenv('SPARK_LAKEHOUSE_DIR_FILES', '/opt/spark/spark-lakehouse/partitioned')
SPARK_LAKEHOUSE_DIR = os.getenv('SPARK_LAKEHOUSE_DIR', '/opt/spark/spark-lakehouse')

@transformer
def transform(*args, **kwargs):
    """
    Transform the DataFrame to ensure correct data types
    """
    start_year = kwargs['start_year']
    start_month = kwargs['start_month']
    end_year = kwargs['end_year']
    end_month = kwargs['end_month']
    pipeline_run_name = kwargs['pipeline_run_name']
    tripdata_type = kwargs['tripdata_type']
    LOG = kwargs.get('logger')

    spark = get_spark_session(
        mode='cluster',
        appname=f'{pipeline_run_name}.{tripdata_type}_spark_transform_yellow_taxi_data_column'
    )

    partitioned_path = f'{SPARK_LAKEHOUSE_DIR}/partitioned/{tripdata_type}/tmp/pq/raw/{pipeline_run_name}'
    dev_path = f'{SPARK_LAKEHOUSE_DIR}/partitioned/{tripdata_type}/tmp/pq/dev/{pipeline_run_name}'
    schema_manager = FHVTripDataSchema(spark)

    stage_path = dev_path

    # Loop over the date range
    start_date = datetime(start_year, start_month, 1)
    last_day = calendar.monthrange(end_year, end_month)[1]
    end_date = datetime(end_year, end_month, last_day)
    
    current_date = start_date
    while current_date <= end_date:
        year = current_date.year
        month = current_date.month

        partition_path = os.path.join(partitioned_path, f'year={year}', f'month={month}')
        LOG.info(f'Partition path: {partition_path}')

        df = spark.read.parquet(partitioned_path)

        # Add year and month columns based on pickup_datetime
        df = df.withColumn("year", pyspark_year(df["pickup_datetime"])) \
            .withColumn("month", pyspark_month(df["pickup_datetime"]))

        # Cast columns to desired types
        df = schema_manager.cast_columns(df)

        df = df.withColumnRenamed("PULocationID", "pu_location_id") \
            .withColumnRenamed("DOLocationID", "do_location_id") \
            .withColumnRenamed("RatecodeID", "ratecode_id") \
            .withColumnRenamed("SR_Flag", "sr_flag") \
            .withColumnRenamed("Affiliated_base_number", "affiliated_base_number") \
            .withColumnRenamed("dropOff_datetime", "dropoff_datetime")

        write_path = os.path.join(dev_path, f'year={year}', f'month={month}')
        df.write.mode("overwrite").parquet(write_path)
        LOG.info(f"Partitioned data for {start_year}-{start_month}:{end_year}-{end_month} written to {stage_path}")

        # Update to next month
        current_date = datetime(year + (month // 12), (month % 12) + 1, 1)

    spark.stop()


@test
def test_columns_name_and_type(*args, **kwargs):
    # Get pipeline config variables and spark
    start_year = kwargs['start_year']
    start_month = kwargs['start_month']
    end_year = kwargs['end_year']
    end_month = kwargs['end_month']
    pipeline_run_name = kwargs['pipeline_run_name']
    tripdata_type = kwargs['tripdata_type']
    LOG = kwargs.get('logger')

    spark = get_spark_session(
        mode='cluster',
        appname=f'test_{pipeline_run_name}_{tripdata_type}_spark_transform_yellow_taxi_data_column'
    )

    # Where the parquet files are contained
    partitioned_path = f'{SPARK_LAKEHOUSE_DIR}/partitioned/{tripdata_type}/tmp/pq/raw/{pipeline_run_name}'
    stage_path = f'{SPARK_LAKEHOUSE_DIR}/partitioned/{tripdata_type}/tmp/pq/dev/{pipeline_run_name}'
    LOG.info(f"Starting schema validation from {start_year}-{start_month} to {end_year}-{end_month}")

    # Updated expected schema with renamed columns
    expected_schema = StructType([
        StructField("dispatching_base_num", StringType(), True),
        StructField("pickup_datetime", TimestampType(), True),
        StructField("dropoff_datetime", TimestampType(), True),
        StructField("pu_location_id", IntegerType(), True),
        StructField("do_location_id", IntegerType(), True),
        StructField("sr_flag", IntegerType(), True),
        StructField("affiliated_base_number", StringType(), True),
    ])
    # Loop over the date range
    start_date = datetime(start_year, start_month, 1)
    end_date = datetime(end_year, end_month, 1)

    current_date = start_date
    while current_date <= end_date:
        year = current_date.year
        month = current_date.month

        LOG.info(f"Validating schema for {year}-{month}")

        # Read the Parquet file for the specific year and month
        df = spark.read.schema(expected_schema).parquet(f"{stage_path}/year={year}/month={month}")

        # Get actual column names and types from the DataFrame
        actual_columns = df.columns
        actual_types = {field.name: field.dataType for field in df.schema.fields}

        # Define expected columns and types
        expected_columns = [field.name for field in expected_schema.fields]
        expected_types = {field.name: field.dataType for field in expected_schema.fields}

        # Check for missing or extra columns
        missing_columns = set(expected_columns) - set(actual_columns)
        extra_columns = set(actual_columns) - set(expected_columns)

        if missing_columns:
            LOG.error(f"Missing columns in {year}-{month}: {missing_columns}")
        if extra_columns:
            LOG.error(f"Extra columns in {year}-{month}: {extra_columns}")

        assert not missing_columns, f"Missing columns in {year}-{month}: {missing_columns}"
        assert not extra_columns, f"Extra columns in {year}-{month}: {extra_columns}"

        # Check for correct data types
        for field in expected_schema.fields:
            column = field.name
            expected_dtype = field.dataType
            actual_dtype = actual_types.get(column)
            if actual_dtype != expected_dtype:
                LOG.error(f"Column '{column}' in {year}-{month} has wrong type: expected {expected_dtype}, got {actual_dtype}")
            assert actual_dtype == expected_dtype, \
                f"Column '{column}' in {year}-{month} has wrong type: expected {expected_dtype}, got {actual_dtype}"

        # Update the counter to the next month
        current_date = datetime(year + (month // 12), (month % 12) + 1, 1)

    LOG.info("Schema validation complete")
    spark.stop()
