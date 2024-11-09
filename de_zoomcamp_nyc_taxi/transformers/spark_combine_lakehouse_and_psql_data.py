if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

from datetime import datetime
from de_zoomcamp_nyc_taxi.utils.spark.spark_util import get_spark_session, get_dataframe_schema, exceeds_data_loss_threshold
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, year as pyspark_year, month as pyspark_month, substring
import os

SPARK_LAKEHOUSE_FILES_DIR = os.getenv('SPARK_LAKEHOUSE_FILES_DIR', '/opt/spark/spark-lakehouse/partitioned')

@transformer
def transform(data, *args, **kwargs):
    """
    Combines two datasets, lakehouse and staging (from PostgreSQL), using a merge strategy. The pipeline reads 
    Parquet files from both sources, adds salt columns to optimize join distribution, and 
    joins them on `dwid` and salt while selecting and renaming columns to avoid conflicts. 
    It then filters out null values, adds `year` and `month` for partitioning, and 
    writes the final DataFrame to a partitioned Parquet output.

    Configuration:
    - year_month (str): Year and month to process, in 'YYYY_MM' format.
    - pipeline_run_name (str): Identifier for this pipeline run.
    - spark_mode (str): Execution mode for Spark.
    - tripdata_type (str): Type of trip data (e.g., green, yellow).
    - data_loss_threshold (float): Acceptable threshold for data loss due to missing values.

    Raises:
    - ValueError: If required parameters are missing or data loss exceeds the threshold.
    """

    LOG = kwargs.get('logger')
    year_month = kwargs['year_month']  # Expected format: '2023_10'
    pipeline_run_name = kwargs['pipeline_run_name']
    spark_mode = kwargs['spark_mode']
    tripdata_type = kwargs['tripdata_type']
    data_loss_threshold = kwargs['data_loss_threshold']

    if not all([year_month, tripdata_type]):
        raise ValueError("Error: 'year_month' and 'tripdata_type' must be provided.")

    year, month = map(int, year_month.split('_'))
    LOG.info(f"Initializing Spark session for pipeline run: {pipeline_run_name} in {spark_mode} mode...")

    appname = f'{pipeline_run_name}_spark_combine_lakehouse_and_psql_data'
    spark = get_spark_session(mode=spark_mode, appname=appname)

    base_tmp_lakehouse_path = os.path.join(SPARK_LAKEHOUSE_FILES_DIR, f'{tripdata_type}/tmp/pq/pre_lakehouse_to_psql_production/{pipeline_run_name}')
    base_tmp_stage_pq_path = os.path.join(SPARK_LAKEHOUSE_FILES_DIR, f'{tripdata_type}/tmp/pq/pre_stage_to_prod_psql/{pipeline_run_name}')
    base_write_path = os.path.join(SPARK_LAKEHOUSE_FILES_DIR, f'{tripdata_type}/tmp/pq/pre_combined_data_production/{pipeline_run_name}')

    LOG.info("Reading data from lakehouse and stage paths...")
    lakehouse_df = spark.read.parquet(os.path.join(base_tmp_lakehouse_path, f'year={year}/month={month}/*.parquet'))
    stage_df = spark.read.parquet(os.path.join(base_tmp_stage_pq_path, f'year={year}/month={month}/*.parquet'))

    LOG.info(f"Lakehouse partitions: {lakehouse_df.rdd.getNumPartitions()}")
    LOG.info(f"Stage partitions: {stage_df.rdd.getNumPartitions()}")

    lakehouse_renamed = lakehouse_df.select([col(c).alias(f"lh_{c}") for c in lakehouse_df.columns])
    stage_renamed = stage_df.select([col(c).alias(f"st_{c}") for c in stage_df.columns])

    LOG.info("Adding salt column to distribute data evenly across partitions...")
    lakehouse_renamed = lakehouse_renamed.withColumn("lh_salt", substring(col("lh_dwid"), -1, 1))
    stage_renamed = stage_renamed.withColumn("st_salt", substring(col("st_dwid"), -1, 1))

    LOG.info("Joining lakehouse and stage data...")
    combined_df = lakehouse_renamed.alias("lh").join(
        stage_renamed.alias("st"),
        on=[col("lh_dwid") == col("st_dwid"), col("lh_salt") == col("st_salt")],
        how="outer"
    ).drop("lh_salt", "st_salt")

    selected_columns = []
    for column in lakehouse_df.columns:
        lh_col = f"lh_{column}"
        st_col = f"st_{column}"
        selected_columns.append(col(lh_col).alias(column) if lh_col in combined_df.columns else col(st_col).alias(column))

    combined_df = combined_df.select(*selected_columns)

    LOG.info("Checking for null values and data loss...")
    null_count = combined_df.filter(col("pickup_datetime").isNull()).count()
    if null_count > 0 and exceeds_data_loss_threshold(combined_df, data_loss_threshold):
        raise ValueError(f"Data loss exceeds the threshold of '{data_loss_threshold}'")
    combined_df = combined_df.filter(col("pickup_datetime").isNotNull())

    LOG.info("Adding year and month columns for partitioning...")
    combined_df = combined_df.withColumn("year", pyspark_year(col("pickup_datetime")))
    combined_df = combined_df.withColumn("month", pyspark_month(col("pickup_datetime")))

    LOG.info("Writing the DataFrame to the output path with partitioning...")
    combined_df.write.mode("overwrite").partitionBy('year', 'month').parquet(base_write_path)

    LOG.info(f"Data export completed with {combined_df.rdd.getNumPartitions()} partitions written.")
    spark.stop()

@test
def test_output(*args, **kwargs) -> None:
    """
    Tests the output for the given year and month by checking the existence and integrity of partitioned Parquet files.
    """
    LOG = kwargs.get('logger')
    year_month = kwargs['year_month']  # Expected format: '2023_10'
    pipeline_run_name = kwargs['pipeline_run_name']
    tripdata_type = kwargs['tripdata_type']
    spark_mode = kwargs['spark_mode']

    LOG.info("Initializing Spark session for testing output...")
    spark = get_spark_session(mode=spark_mode, appname=f'test.{pipeline_run_name}_spark_combine_lakehouse_and_psql_data')

    year, month = map(int, year_month.split('_'))
    partition_path = os.path.join(
        SPARK_LAKEHOUSE_FILES_DIR,
        f'{tripdata_type}/tmp/pq/pre_combined_data_production/{pipeline_run_name}',
        f'year={year}/month={month}'
    )

    LOG.info(f"Testing data for {year}-{month:02d}")
    LOG.info(f"Partition path: {partition_path}")

    if not os.path.exists(partition_path):
        raise FileNotFoundError(f"Partition path does not exist: {partition_path}")

    parquet_files = [f for f in os.listdir(partition_path) if f.endswith('.parquet')]
    if not parquet_files:
        raise FileNotFoundError(f"No Parquet files found in partition path: {partition_path}")

    LOG.info(f"Found {len(parquet_files)} Parquet files in partition path: {partition_path}")

    LOG.info("All data range tests completed successfully.")
    spark.stop()
