if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

import os
from os import path
from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from de_zoomcamp_nyc_taxi.utils.spark.spark_util import get_spark_session, get_dataframe_schema
from pyspark.sql.functions import col, lit
from pyspark.sql.functions import concat_ws, sha2
from datetime import datetime

SPARK_LAKEHOUSE_FILES_DIR = os.getenv('SPARK_LAKEHOUSE_DIR_FILES', '/opt/spark/spark-lakehouse/partitioned')
SPARK_LAKEHOUSE_DIR = os.getenv('SPARK_LAKEHOUSE_DIR', '/opt/spark/spark-lakehouse')

@transformer
def transform(data, *args, **kwargs):
    """
    Transform the data by adding a unique, consistent identifier ('dwid').
    """
    LOG = kwargs.get('logger')
    start_year = kwargs['start_year']
    start_month = kwargs['start_month']
    end_year = kwargs['end_year']
    end_month = kwargs['end_month']
    pipeline_run_name = kwargs['pipeline_run_name']
    tripdata_type = kwargs['tripdata_type']
    spark_mode = kwargs['spark_mode']

    if not all([start_year, start_month, end_year, end_month, tripdata_type]):
        raise ValueError("Error: 'start_year', 'start_month', 'end_year', 'end_month', and 'tripdata_type' must be provided.")

    base_stage_path = os.path.join(
        SPARK_LAKEHOUSE_FILES_DIR,
        f'{tripdata_type}/tmp/pq/stage/{pipeline_run_name}'
    )
    current_date = datetime(start_year, start_month, 1)
    end_date = datetime(end_year, end_month, 1)

    spark = get_spark_session(mode=spark_mode, appname="spark_add_unique_identifier")

    while current_date <= end_date:
        year = current_date.year
        month = current_date.month

        partition_path = os.path.join(base_stage_path, f'year={year}', f'month={month}')
        LOG.info(f"Starting transformation for {tripdata_type} data for {year}-{month}.")
        LOG.info(f"Partition path: {partition_path}")

        if not os.path.exists(partition_path):
            raise FileNotFoundError(f"Partition path does not exist: {partition_path}")

        df = spark.read.schema(get_dataframe_schema(spark, partition_path)).parquet(partition_path)

        # Drop duplicates based on unique identifiers
        df = df.dropDuplicates(["pickup_datetime", "dropoff_datetime", "pu_location_id", "do_location_id"])

        # Create a unique key based on the unique columns
        df = df.withColumn('unique_key', concat_ws('_',
            col('pickup_datetime').cast("string"),
            col('dropoff_datetime').cast("string"),
            col('pu_location_id').cast("string"),
            col('do_location_id').cast("string")
        ))

        # Generate 'dwid' by hashing the unique key
        df = df.withColumn('dwid', sha2(col('unique_key'), 256).substr(1, 16))

        # Drop the 'unique_key' column
        df = df.drop('unique_key')

        # Select columns in desired order
        desired_column_order = ['dwid'] + [col_name for col_name in df.columns if col_name != 'dwid']
        df = df.select(*desired_column_order)

        # Write the dataframe back to the same partition path
        write_path = partition_path
        df.write.parquet(write_path, mode="overwrite")

        LOG.info(f"Transformation completed. Data written to {write_path}.")

        # Move to the next month
        if month == 12:
            year += 1
            month = 1
        else:
            month += 1
        current_date = datetime(year, month, 1)

    spark.stop()

@test
def test_output(*args, **kwargs) -> None:
    """
    Test the output of the parquet data loading process.
    """
    LOG = kwargs.get('logger')
    start_year = kwargs['start_year']
    start_month = kwargs['start_month']
    end_year = kwargs['end_year']
    end_month = kwargs['end_month']
    pipeline_run_name = kwargs['pipeline_run_name']
    tripdata_type = kwargs['tripdata_type']
    spark_mode = kwargs['spark_mode']

    spark = get_spark_session(mode=spark_mode, appname='test_spark_add_unique_identifier')

    current_date = datetime(start_year, start_month, 1)
    end_date = datetime(end_year, end_month, 1)
    
    while current_date <= end_date:
        year = current_date.year
        month = current_date.month
        year_month_str = f'year={year}/month={month}'

        base_stage_path = os.path.join(
            SPARK_LAKEHOUSE_FILES_DIR,
            f'{tripdata_type}/tmp/pq/stage/{pipeline_run_name}'
        )
        partition_path = os.path.join(base_stage_path, year_month_str)

        LOG.info(f"Testing data for {year}-{month}")
        LOG.info(f"Partition path: {partition_path}")

        if not os.path.exists(partition_path):
            raise FileNotFoundError(f"Partition path does not exist: {partition_path}")

        df = spark.read.schema(get_dataframe_schema(spark, partition_path)).parquet(partition_path)

        assert 'dwid' in df.columns, f"Year {year}, Month {month:02d}: The output DataFrame does not contain 'dwid' column"
        
        dwid_count = df.select('dwid').distinct().count()
        row_count = df.count()
        assert dwid_count == row_count, f"Year {year}, Month {month:02d}: The 'dwid' column is not unique"

        # Move to the next month
        if month == 12:
            year += 1
            month = 1
        else:
            month += 1
        current_date = datetime(year, month, 1)

    LOG.info("All data range tests completed successfully")
    spark.stop()
