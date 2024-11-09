if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

import os
from os import path
from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from de_zoomcamp_nyc_taxi.utils.spark.spark_util import get_spark_session, get_dataframe_schema
from pyspark.sql.functions import col, concat_ws, sha2

SPARK_LAKEHOUSE_FILES_DIR = os.getenv('SPARK_LAKEHOUSE_DIR_FILES', '/opt/spark/spark-lakehouse/partitioned')
SPARK_LAKEHOUSE_DIR = os.getenv('SPARK_LAKEHOUSE_DIR', '/opt/spark/spark-lakehouse')

@transformer
def transform(data, *args, **kwargs):
    """
    Transform the data by adding a unique, consistent identifier ('dwid').
    """
    LOG = kwargs.get('logger')
    year_month = kwargs['year_month']
    pipeline_run_name = kwargs['pipeline_run_name']
    tripdata_type = kwargs['tripdata_type']
    spark_mode = kwargs['spark_mode']

    year, month = map(int, year_month.split('_'))

    base_stage_path = os.path.join(
        SPARK_LAKEHOUSE_FILES_DIR,
        f'{tripdata_type}/tmp/pq/stage/{pipeline_run_name}'
    )
    partition_path = os.path.join(base_stage_path, f'year={year}', f'month={month}')
    
    spark = get_spark_session(mode=spark_mode, appname="spark_add_unique_identifier")
    LOG.info(f"Starting transformation for {tripdata_type} data for {year}-{month}.")
    LOG.info(f"Partition path: {partition_path}")

    if not os.path.exists(partition_path):
        raise FileNotFoundError(f"Partition path does not exist: {partition_path}")

    df = spark.read.schema(get_dataframe_schema(spark, partition_path)).parquet(partition_path)

    # Drop duplicates based on unique identifiers
    df = df.dropDuplicates(["pickup_datetime", "dropoff_datetime", "pu_location_id", "do_location_id"])

    df = df.withColumn('unique_key', concat_ws('_',
        col('pickup_datetime').cast("string"),
        col('dropoff_datetime').cast("string"),
        col('pu_location_id').cast("string"),
        col('do_location_id').cast("string")
    )).withColumn('dwid', sha2(col('unique_key'), 256).substr(1, 16)).drop('unique_key')

    desired_column_order = ['dwid'] + [col_name for col_name in df.columns if col_name != 'dwid']
    df = df.select(*desired_column_order)

    df = df.cache() 

    df.write.parquet(partition_path, mode="overwrite")
    LOG.info(f"Transformation completed. Data written to {partition_path}.")

    spark.stop()

@test
def test_output(*args, **kwargs) -> None:
    """
    Test the output of the parquet data loading process.
    """
    LOG = kwargs.get('logger')
    year_month = kwargs['year_month']
    pipeline_run_name = kwargs['pipeline_run_name']
    tripdata_type = kwargs['tripdata_type']
    spark_mode = kwargs['spark_mode']
    
    year, month = map(int, year_month.split('_'))

    base_stage_path = os.path.join(
        SPARK_LAKEHOUSE_FILES_DIR,
        f'{tripdata_type}/tmp/pq/stage/{pipeline_run_name}'
    )
    partition_path = os.path.join(base_stage_path, f'year={year}', f'month={month}')

    spark = get_spark_session(mode=spark_mode, appname='test_spark_add_unique_identifier')

    LOG.info(f"Testing data for {year}-{month}")
    LOG.info(f"Partition path: {partition_path}")

    if not os.path.exists(partition_path):
        raise FileNotFoundError(f"Partition path does not exist: {partition_path}")

    df = spark.read.schema(get_dataframe_schema(spark, partition_path)).parquet(partition_path)

    # Verify that 'dwid' column exists and contains unique values
    assert 'dwid' in df.columns, f"The output DataFrame does not contain 'dwid' column for {year}-{month}"
    dwid_count = df.select('dwid').distinct().count()
    row_count = df.count()
    assert dwid_count == row_count, f"The 'dwid' column is not unique for {year}-{month}"

    LOG.info("Data test completed successfully for specified year-month")
    spark.stop()
