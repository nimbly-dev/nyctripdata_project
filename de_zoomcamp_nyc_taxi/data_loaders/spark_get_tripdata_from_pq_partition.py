import os
from datetime import datetime
from de_zoomcamp_nyc_taxi.utils.spark.spark_util import get_spark_session, get_dataframe_schema
from pyspark.sql.functions import col

SPARK_LAKEHOUSE_DIR = os.getenv('SPARK_LAKEHOUSE_DIR', '/opt/spark/spark-lakehouse')

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

@data_loader
def load_data_from_parquet(*args, **kwargs):
    LOG = kwargs.get('logger')
    year_month = kwargs['year_month']
    pipeline_run_name = kwargs['pipeline_run_name']
    tripdata_type = kwargs['tripdata_type']
    spark_mode = kwargs['spark_mode']

    target_pq_path = kwargs['configuration'].get('target_pq_path')
    source_pq_path = kwargs['configuration'].get('source_pq_path')

    year, month = map(int, year_month.split('_'))
    base_source_path = f'{SPARK_LAKEHOUSE_DIR}/partitioned/{tripdata_type}/tmp/pq/{source_pq_path}/{pipeline_run_name}'
    base_target_path = f'{SPARK_LAKEHOUSE_DIR}/partitioned/{tripdata_type}/tmp/pq/{target_pq_path}/{pipeline_run_name}'

    spark = get_spark_session(appname=f'{pipeline_run_name}.{tripdata_type}_spark_load_and_transform_nyc_taxi_data')
    LOG.info("Spark session initiated.")

    partition_path = os.path.join(base_source_path, f'year={year}', f'month={month}')
    write_path = os.path.join(base_target_path, f'year={year}', f'month={month}')
    LOG.info(f"Processing data for {year}-{month}")
    LOG.info(f"Source partition path: {partition_path}")

    if not os.path.exists(partition_path):
        raise FileNotFoundError(f"Source partition path does not exist: {partition_path}")

    df = spark.read.schema(get_dataframe_schema(spark, partition_path)).parquet(partition_path)
    df.write.mode("overwrite").parquet(write_path)
    LOG.info(f"Partitioned data for {year}-{month} written to {write_path}")

    LOG.info(f"Data loading completed for pipeline run {pipeline_run_name} for {year}-{month}")
    spark.stop()

@test
def test_output(*args, **kwargs) -> None:
    LOG = kwargs.get('logger')
    year_month = kwargs['year_month']
    pipeline_run_name = kwargs['pipeline_run_name']
    tripdata_type = kwargs['tripdata_type']
    target_pq_path = kwargs['configuration'].get('target_pq_path')

    year, month = map(int, year_month.split('_'))
    base_target_path = f'{SPARK_LAKEHOUSE_DIR}/partitioned/{tripdata_type}/tmp/pq/{target_pq_path}/{pipeline_run_name}'

    spark = get_spark_session(appname='test_spark_get_tripdata_from_stage')

    partition_path = os.path.join(base_target_path, f'year={year}', f'month={month}')
    LOG.info(f"Testing data for {year}-{month}")
    LOG.info(f"Partition path: {partition_path}")

    if not os.path.exists(partition_path):
        raise FileNotFoundError(f"Partition path does not exist: {partition_path}")

    df = spark.read.schema(get_dataframe_schema(spark, partition_path)).parquet(partition_path)

    assert df.count() > 0, f"DataFrame for {year}-{month:02d} is empty"
    assert df.columns, f"DataFrame for {year}-{month:02d} has no columns"

    LOG.info("Data test completed successfully for specified year-month")
    spark.stop()
