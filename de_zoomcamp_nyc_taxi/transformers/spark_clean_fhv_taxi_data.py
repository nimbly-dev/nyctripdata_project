import logging
import os
from mage_ai.data_preparation.repo_manager import get_repo_path
from de_zoomcamp_nyc_taxi.utils.spark.spark_util import get_spark_session
from pyspark.sql.functions import col, year as pyspark_year, month as pyspark_month
from de_zoomcamp_nyc_taxi.utils.spark.spark_util import get_dataframe_schema
from de_zoomcamp_nyc_taxi.utils.common.common_util import validate_parquet_files
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType
from datetime import datetime

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

logger = logging.getLogger(__name__)

SPARK_LAKEHOUSE_DIR = os.getenv('SPARK_LAKEHOUSE_DIR', '/opt/spark/spark-lakehouse')

@transformer
def transform(data, *args, **kwargs):
    year_month = kwargs['year_month']
    pipeline_run_name = kwargs['pipeline_run_name']
    tripdata_type = kwargs['tripdata_type']
    spark_mode = kwargs['spark_mode']
    LOG = kwargs.get('logger')
    
    year, month = map(int, year_month.split('_'))
    pq_dir = kwargs['configuration'].get('pq_dir')
    base_stage_path = f'{SPARK_LAKEHOUSE_DIR}/partitioned/{tripdata_type}/tmp/pq/{pq_dir}/{pipeline_run_name}'

    spark = get_spark_session(
        mode='cluster',
        appname=f'{pipeline_run_name}.{tripdata_type}_spark_clean_yellow_taxi_data'
    )
    LOG.info("Spark session initiated.")

    partition_path = os.path.join(base_stage_path, f'year={year}', f'month={month}')
    LOG.info(f'Partition path: {partition_path}')

    if not validate_parquet_files(partition_path):
        raise FileNotFoundError(f"No valid Parquet files found in directory: {partition_path}")

    df = spark.read.schema(get_dataframe_schema(spark, partition_path)).parquet(partition_path)

    df = (
        df.fillna({"sr_flag": 0})
        .dropna(subset=["pu_location_id", "do_location_id"])
        .withColumn("pickup_datetime", F.to_timestamp("pickup_datetime").cast(TimestampType()))
        .withColumn("dropoff_datetime", F.to_timestamp("dropoff_datetime").cast(TimestampType()))
        .dropDuplicates()
        .filter(F.col("dropoff_datetime") > F.col("pickup_datetime"))
        .filter(F.col("sr_flag").isin(0, 1))
    )

    LOG.info(f"Number of records after deduplication: {df.count()}")
    df.write.mode("overwrite").parquet(partition_path)
    LOG.info(f"Writing cleaned data for {year}-{month} to Parquet files at: {partition_path}")
    
    LOG.info("Clean complete")
    spark.stop()

@test
def test_all_conditions(*args, **kwargs) -> None:
    year_month = kwargs['year_month']
    pipeline_run_name = kwargs['pipeline_run_name']
    tripdata_type = kwargs['tripdata_type']
    pq_dir = kwargs['configuration'].get('pq_dir')
    spark_mode = kwargs['spark_mode']

    year, month = map(int, year_month.split('_'))
    base_stage_path = f'{SPARK_LAKEHOUSE_DIR}/partitioned/{tripdata_type}/tmp/pq/{pq_dir}/{pipeline_run_name}'
    spark = get_spark_session(mode=spark_mode, appname='test_all_conditions_spark_clean_yellow_taxi_data')

    partition_path = os.path.join(base_stage_path, f'year={year}', f'month={month}')
    
    if os.path.exists(partition_path):
        df = spark.read.schema(get_dataframe_schema(spark, partition_path)).parquet(partition_path)

        duplicate_count = df.count() - df.dropDuplicates().count()
        if duplicate_count > 0:
            print(f"{duplicate_count} duplicate rows found in partition for {year}-{month}.")
            duplicates = df.groupBy(['pickup_datetime', 'dropoff_datetime', 'pu_location_id', 'do_location_id']).count().filter("count > 1")
            duplicates.show()

        assert df.count() == df.dropDuplicates().count(), f"Duplicate rows found in partition {year}-{month}"

        assert df.filter(
            F.col('pickup_datetime') == F.col('dropoff_datetime')
        ).count() == 0, f"Same pickup and dropoff times found in partition {year}-{month}"

        assert df.filter(
            ~F.col('sr_flag').isin(0, 1)
        ).count() == 0, f"Invalid SR_Flag values found in partition {year}-{month}"

        assert df.filter(
            F.col('pu_location_id').isNull() | F.col('do_location_id').isNull()
        ).count() == 0, f"Null values found in pu_location_id or do_location_id in partition {year}-{month}"
        
        assert df.filter(F.col("pu_location_id").isNull()).count() == 0, f"Null values found in pu_location_id in partition {year}-{month}"
        assert df.filter(F.col("do_location_id").isNull()).count() == 0, f"Null values found in do_location_id in partition {year}-{month}"
    
    spark.stop()
