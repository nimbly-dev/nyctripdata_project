import logging
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year as pyspark_year, month as pyspark_month, lit
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType, TimestampType
from de_zoomcamp_nyc_taxi.utils.spark.spark_util import get_spark_session
from de_zoomcamp_nyc_taxi.model.schema.green_tripdata import GreenTripDataSchema
from de_zoomcamp_nyc_taxi.utils.common.common_util import validate_parquet_files
from datetime import datetime

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

logger = logging.getLogger(__name__)

SPARK_LAKEHOUSE_FILES_DIR = os.getenv('SPARK_LAKEHOUSE_DIR_FILES', '/opt/spark/spark-lakehouse/partitioned')
SPARK_LAKEHOUSE_DIR = os.getenv('SPARK_LAKEHOUSE_DIR', '/opt/spark/spark-lakehouse')

@transformer
def transform(*args, **kwargs):
    year_month = kwargs['year_month']
    pipeline_run_name = kwargs['pipeline_run_name']
    tripdata_type = 'green_cab_tripdata'
    LOG = kwargs.get('logger')

    year, month = map(int, year_month.split('_'))

    spark = get_spark_session(
        mode='cluster',
        appname=f'{pipeline_run_name}_{tripdata_type}_spark_transform_green_taxi_data'
    )

    partitioned_path = f'{SPARK_LAKEHOUSE_DIR}/partitioned/{tripdata_type}/tmp/pq/raw/{pipeline_run_name}'
    dev_path = f'{SPARK_LAKEHOUSE_DIR}/partitioned/{tripdata_type}/tmp/pq/dev/{pipeline_run_name}'
    green_tripdata_schema = GreenTripDataSchema(spark_session=spark)

    LOG.info(f"Processing data for {year}-{month}")
    partition_path = os.path.join(partitioned_path, f'year={year}', f'month={month}')
    LOG.info(f'Partition path: {partition_path}')

    if not validate_parquet_files(partition_path):
        raise FileNotFoundError(f"No valid Parquet files found in directory: {partition_path}")

    df = spark.read.parquet(partition_path)
    df = green_tripdata_schema.cast_columns(df)

    df = df.withColumn("year", pyspark_year(df["lpep_pickup_datetime"])) \
           .withColumn("month", pyspark_month(df["lpep_pickup_datetime"])) \
           .drop("ehail_fee")

    df = df.withColumnRenamed("PULocationID", "pu_location_id") \
           .withColumnRenamed("DOLocationID", "do_location_id") \
           .withColumnRenamed("lpep_pickup_datetime", "pickup_datetime") \
           .withColumnRenamed("lpep_dropoff_datetime", "dropoff_datetime") \
           .withColumnRenamed("RatecodeID", "ratecode_id") \
           .withColumnRenamed("VendorID", "vendor_id")

    write_path = os.path.join(dev_path, f'year={year}', f'month={month}')
    df.write.mode("overwrite").parquet(write_path)
    LOG.info(f"Data written to {write_path}")

    LOG.info("Green Taxi Transformation complete")
    spark.stop()

@test
def test_columns_name_and_type(*args, **kwargs):
    year_month = kwargs['year_month']
    pipeline_run_name = kwargs['pipeline_run_name']
    tripdata_type = 'green_cab_tripdata'
    LOG = kwargs.get('logger')

    year, month = map(int, year_month.split('_'))

    spark = get_spark_session(
        mode='cluster',
        appname=f'test_{pipeline_run_name}_{tripdata_type}_spark_transform_green_taxi_data'
    )

    stage_path = f'{SPARK_LAKEHOUSE_DIR}/partitioned/{tripdata_type}/tmp/pq/dev/{pipeline_run_name}'
    LOG.info(f"Validating schema for {year}-{month}")

    expected_schema = StructType([
        StructField("vendor_id", IntegerType(), True),
        StructField("pickup_datetime", TimestampType(), True),
        StructField("dropoff_datetime", TimestampType(), True),
        StructField("passenger_count", IntegerType(), True),
        StructField("trip_distance", FloatType(), True),
        StructField("ratecode_id", IntegerType(), True),
        StructField("store_and_fwd_flag", StringType(), True),
        StructField("pu_location_id", IntegerType(), True),
        StructField("do_location_id", IntegerType(), True),
        StructField("payment_type", IntegerType(), True),
        StructField("fare_amount", FloatType(), True),
        StructField("extra", FloatType(), True),
        StructField("mta_tax", FloatType(), True),
        StructField("tip_amount", FloatType(), True),
        StructField("tolls_amount", FloatType(), True),
        StructField("improvement_surcharge", FloatType(), True),
        StructField("total_amount", FloatType(), True),
        StructField("trip_type", IntegerType(), True),
        StructField("congestion_surcharge", FloatType(), True),
    ])

    df = spark.read.schema(expected_schema).parquet(f"{stage_path}/year={year}/month={month}")

    actual_columns = df.columns
    actual_types = {field.name: field.dataType for field in df.schema.fields}
    expected_columns = [field.name for field in expected_schema.fields]
    expected_types = {field.name: field.dataType for field in expected_schema.fields}

    missing_columns = set(expected_columns) - set(actual_columns)
    extra_columns = set(actual_columns) - set(expected_columns)

    if missing_columns:
        LOG.error(f"Missing columns: {missing_columns}")
    if extra_columns:
        LOG.error(f"Extra columns: {extra_columns}")

    assert not missing_columns, f"Missing columns: {missing_columns}"
    assert not extra_columns, f"Extra columns: {extra_columns}"

    for field in expected_schema.fields:
        column = field.name
        expected_dtype = field.dataType
        actual_dtype = actual_types.get(column)
        if actual_dtype != expected_dtype:
            LOG.error(f"Column '{column}' has wrong type: expected {expected_dtype}, got {actual_dtype}")
        assert actual_dtype == expected_dtype, \
            f"Column '{column}' has wrong type: expected {expected_dtype}, got {actual_dtype}"

    LOG.info("Schema validation complete")
    spark.stop()
