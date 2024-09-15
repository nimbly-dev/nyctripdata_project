from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year as pyspark_year, month as pyspark_month, lit
import os
from os import path
from pyspark.sql.functions import col, year as pyspark_year, month as pyspark_month
from de_zoomcamp_nyc_taxi.model.schema.yellow_tripdata import YellowTripDataSchema
from de_zoomcamp_nyc_taxi.model.schema.green_tripdata import GreenTripDataSchema
from de_zoomcamp_nyc_taxi.utils.spark.spark_util import get_spark_session

SPARK_PARTITION_FILES_DIR = os.getenv('SPARK_PARTITION_FILES_DIR', '/opt/spark/spark-warehouse/partitioned')
SPARK_WAREHOUSE_DIR = os.getenv('SPARK_WAREHOUSE_DIR', '/opt/spark/spark-warehouse')

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

@data_loader
def load_data_from_postgres(*args, **kwargs):
    year = kwargs['year']
    month = kwargs['month']
    spark_mode = kwargs.get('spark_mode', 'local')
    tripdata_type = kwargs['tripdata_type']
    stage_dbname = kwargs['configuration'].get('stage_dbname')
    stage_databaseurl = kwargs['configuration'].get('stage_databaseurl')

    # PostgreSQL connection parameters
    schema_name = kwargs['configuration'].get('schema')
    table_name = f'{kwargs["configuration"].get("table_name")}_staging_{year}_{month:02d}'  # Targeting the specific partition table

    cluster_additional_configs = {
        'spark.jars': '/opt/spark/third-party-jars/postgresql-42.2.24.jar',
        'spark.driver.extraClassPath': '/opt/spark/third-party-jars/postgresql-42.2.24.jar'
    }
    base_stage_path = os.path.join(SPARK_PARTITION_FILES_DIR, f'{tripdata_type}/pq/stage/{year}-{month:02d}')
    
    spark = get_spark_session(mode=spark_mode, additional_configs=cluster_additional_configs, appname='spark_get_tripdata_from_stage_psql')

    partition_column = "pickup_datetime"
    lower_bound = f"{year}-{month:02d}-01 00:00:00"
    upper_bound = f"{year}-{month + 1:02d}-01 00:00:00" if month < 12 else f"{year + 1}-01-01 00:00:00"
    # num_partitions = 3  # Adjust this based on your cluster

    print("Starting read")
    # Directly read the specific partition table
    df = spark.read \
        .format("jdbc") \
        .option("url", f'jdbc:postgresql://{stage_databaseurl}/{stage_dbname}') \
        .option("dbtable", f"{schema_name}.{table_name}") \
        .option("user", "postgres") \
        .option("password", "postgres") \
        .option("driver", "org.postgresql.Driver") \
        .option("partitionColumn", partition_column) \
        .option("lowerBound", lower_bound) \
        .option("upperBound", upper_bound) \
        .load()

    df = df.withColumn("year", lit(year)) \
           .withColumn("month", lit(month))

    # df = df.coalesce(6)  # Adjust the number of partitions before writing

    print("Starting write")
    df.write.partitionBy("year", "month").parquet(base_stage_path, mode="overwrite")

    spark.stop()

def read_parquet(*args, **kwargs):
    """Helper function to read parquet files based on the year and month."""
    year = kwargs['year']
    month = kwargs['month']

    # Spark Partition parameters
    table_name = kwargs['configuration'].get('table_name')

    base_stage_path = os.path.join(SPARK_PARTITION_FILES_DIR, f'{table_name}/pq/stage/{year}-{month:02d}')
    partition_path = os.path.join(base_stage_path, f'year={year}', f'month={month}')

    spark = get_spark_session(mode='cluster', additional_configs={}, appname='read_parquet')

    yellow_tripdata_schema = YellowTripDataSchema(spark_session=spark)

    df = spark.read.schema(yellow_tripdata_schema.get_dataframe_schema(partition_path)).parquet(partition_path)

    spark.stop()
    return df

@test
def test_output(*args, **kwargs) -> None:
    """
    Template code for testing the output of the block.
    """
    year = kwargs['year']
    month = kwargs['month']

    df = read_parquet(*args, **kwargs)

    assert df.count() > 0, "DataFrame is empty"
