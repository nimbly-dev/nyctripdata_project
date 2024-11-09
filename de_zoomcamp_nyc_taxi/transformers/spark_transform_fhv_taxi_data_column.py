import logging
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year as pyspark_year, month as pyspark_month
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType, TimestampType
from de_zoomcamp_nyc_taxi.model.schema.fhv_tripdata import FHVTripDataSchema
from de_zoomcamp_nyc_taxi.utils.spark.spark_util import get_spark_session
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
    tripdata_type = kwargs['tripdata_type']
    LOG = kwargs.get('logger')

    year, month = map(int, year_month.split('_'))
    spark = get_spark_session(
        mode='cluster',
        appname=f'{pipeline_run_name}.{tripdata_type}_spark_transform_fhv_data'
    )

    partitioned_path = f'{SPARK_LAKEHOUSE_DIR}/partitioned/{tripdata_type}/tmp/pq/raw/{pipeline_run_name}'
    dev_path = f'{SPARK_LAKEHOUSE_DIR}/partitioned/{tripdata_type}/tmp/pq/dev/{pipeline_run_name}'
    schema_manager = FHVTripDataSchema(spark)

    partition_path = os.path.join(partitioned_path, f'year={year}', f'month={month}')
    LOG.info(f'Partition path: {partition_path}')

    if not validate_parquet_files(partition_path):
        raise FileNotFoundError(f"No valid Parquet files found in directory: {partition_path}")

    df = spark.read.parquet(partition_path)

    df = df.withColumn("year", pyspark_year(df["pickup_datetime"])) \
           .withColumn("month", pyspark_month(df["pickup_datetime"]))

    df = schema_manager.cast_columns(df)

    df = df.withColumnRenamed("PULocationID", "pu_location_id") \
           .withColumnRenamed("DOLocationID", "do_location_id") \
           .withColumnRenamed("RatecodeID", "ratecode_id") \
           .withColumnRenamed("SR_Flag", "sr_flag") \
           .withColumnRenamed("Affiliated_base_number", "affiliated_base_number") \
           .withColumnRenamed("dropOff_datetime", "dropoff_datetime")

    write_path = os.path.join(dev_path, f'year={year}', f'month={month}')
    df.write.mode("overwrite").parquet(write_path)
    LOG.info(f"Partitioned data for {year}-{month} written to {write_path}")

    spark.stop()

@test
def test_columns_name_and_type(*args, **kwargs):
    year_month = kwargs['year_month']
    pipeline_run_name = kwargs['pipeline_run_name']
    tripdata_type = kwargs['tripdata_type']
    LOG = kwargs.get('logger')

    year, month = map(int, year_month.split('_'))
    spark = get_spark_session(
        mode='cluster',
        appname=f'test_{pipeline_run_name}_{tripdata_type}_spark_transform_fhv_data'
    )

    stage_path = f'{SPARK_LAKEHOUSE_DIR}/partitioned/{tripdata_type}/tmp/pq/dev/{pipeline_run_name}'
    LOG.info(f"Starting schema validation for {year}-{month}")

    expected_schema = StructType([
        StructField("dispatching_base_num", StringType(), True),
        StructField("pickup_datetime", TimestampType(), True),
        StructField("dropoff_datetime", TimestampType(), True),
        StructField("pu_location_id", IntegerType(), True),
        StructField("do_location_id", IntegerType(), True),
        StructField("sr_flag", IntegerType(), True),
        StructField("affiliated_base_number", StringType(), True),
    ])

    partition_path = os.path.join(stage_path, f'year={year}', f'month={month}')
    df = spark.read.schema(expected_schema).parquet(partition_path)

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
