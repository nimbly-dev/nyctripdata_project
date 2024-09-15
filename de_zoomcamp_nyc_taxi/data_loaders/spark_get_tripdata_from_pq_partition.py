from pyspark.sql import SparkSession
import os
from pyspark.sql.functions import col, year as pyspark_year, month as pyspark_month
from datetime import datetime

from de_zoomcamp_nyc_taxi.utils.spark.spark_util import get_spark_session, get_dataframe_schema, cache_and_delete_files

SPARK_LAKEHOUSE_FILES_DIR = os.getenv('SPARK_LAKEHOUSE_DIR_FILES', '/opt/spark/spark-lakehouse/partitioned')
SPARK_LAKEHOUSE_DIR = os.getenv('SPARK_LAKEHOUSE_DIR', '/opt/spark/spark-lakehouse')

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_parquet(*args, **kwargs):
    LOG = kwargs.get('logger')
    start_year = kwargs['start_year']
    start_month = kwargs['start_month']
    end_year = kwargs['end_year']
    end_month = kwargs['end_month']
    pipeline_run_name = kwargs['pipeline_run_name']
    spark_mode = kwargs['spark_mode']
    tripdata_type = kwargs['tripdata_type']

    target_pq_path = kwargs['configuration'].get('target_pq_path')
    source_pq_path = kwargs['configuration'].get('source_pq_path')

    if not start_year or not start_month or not end_year or not end_month or not tripdata_type:
        raise ValueError("Error: 'start_year', 'start_month', 'end_year', 'end_month', and 'tripdata_type' must be provided.")

    base_source_path = f'{SPARK_LAKEHOUSE_DIR}/partitioned/{tripdata_type}/tmp/pq/{source_pq_path}/{pipeline_run_name}'
    base_target_path = f'{SPARK_LAKEHOUSE_DIR}/partitioned/{tripdata_type}/tmp/pq/{target_pq_path}/{pipeline_run_name}'

    spark = get_spark_session(
        mode=spark_mode,
        appname=f'{pipeline_run_name}.{tripdata_type}_spark_load_and_transform_nyc_taxi_data'
    )
    LOG.info("Spark session initiated.")

    start_date = datetime(start_year, start_month, 1)
    end_date = datetime(end_year, end_month, 31)
    
    current_date = start_date
    while current_date <= end_date:
        year = current_date.year
        month = current_date.month

        partition_path = os.path.join(base_source_path, f'year={year}', f'month={month}')
        write_path = os.path.join(base_target_path, f'year={year}', f'month={month}')
        LOG.info(f"Processing data for {year}-{month}")
        LOG.info(f"Source partition path: {partition_path}")

        if not os.path.exists(partition_path):
            raise FileNotFoundError(f"Source partition path does not exist: {partition_path}")

        df = spark.read.schema(get_dataframe_schema(spark, partition_path)).parquet(partition_path)
        writePath = '{base_target_path}/year={year}/month={month}'

        df.write.mode("overwrite").parquet(write_path)
        LOG.info(f"Partitioned data for {year}-{month} written to {base_target_path}")

        LOG.info(f"Data written to Parquet files at: {base_target_path}")

        current_date = datetime(year + (month // 12), (month % 12) + 1, 1)

    LOG.info(f"Data loading completed for pipeline run {pipeline_run_name} from {start_year}{start_month} to {end_year}{end_month}")
    spark.stop()


@test
def test_output(*args, **kwargs) -> None:
    LOG = kwargs.get('logger')
    start_year = kwargs['start_year']
    start_month = kwargs['start_month']
    end_year = kwargs['end_year']
    end_month = kwargs['end_month']
    pipeline_run_name = kwargs['pipeline_run_name']
    spark_mode = kwargs['spark_mode']
    tripdata_type = kwargs['tripdata_type']
    target_pq_path = kwargs['configuration'].get('target_pq_path')

    base_target_path = f'{SPARK_LAKEHOUSE_DIR}/partitioned/{tripdata_type}/tmp/pq/{target_pq_path}/{pipeline_run_name}'
    spark = get_spark_session(
        mode=spark_mode,
        appname='test_spark_get_tripdata_from_stage'
    )

    current_date = datetime(start_year, start_month, 1)
    end_date = datetime(end_year, end_month, 1)
    
    while current_date <= end_date:
        year = current_date.year
        month = current_date.month
        year_month_str = f'year={year}/month={month}'

        partition_path = os.path.join(base_target_path, f'year={year}', f'month={month}')

        LOG.info(f"Testing data for {year}-{month}")
        LOG.info(f"Partition path: {partition_path}")

        if not os.path.exists(partition_path):
            raise FileNotFoundError(f"Partition path does not exist: {partition_path}")

        df = spark.read.schema(get_dataframe_schema(spark, partition_path)).parquet(partition_path)

        assert df.count() > 0, f"DataFrame for {year}-{month:02d} is empty"
        assert df.columns, f"DataFrame for {year}-{month:02d} has no columns"

        current_date = datetime(year + (month // 12), (month % 12) + 1, 1)

    LOG.info("All data range tests completed successfully")
    spark.stop()
