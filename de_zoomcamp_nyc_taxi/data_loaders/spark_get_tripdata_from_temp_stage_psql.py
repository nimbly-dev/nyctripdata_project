from pyspark.sql import SparkSession
import os
from os import path
from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from pyspark.sql.functions import col, year as pyspark_year, month as pyspark_month
from de_zoomcamp_nyc_taxi.model.schema.yellow_tripdata import YellowTripDataSchema
from de_zoomcamp_nyc_taxi.utils.spark.spark_util import get_spark_session
from de_zoomcamp_nyc_taxi.utils.sql.sql_util import run_sql_on_postgres


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
    spark_mode = kwargs['spark_mode']

    tripdata_type = kwargs['configuration'].get('tripdata_type')

    # PostgreSQL connection parameters
    schema_name = kwargs['configuration'].get('schema_name')
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = kwargs['configuration'].get('config_profile')
    staging_temp_table = f"{schema_name}.{tripdata_type}_{year}_{month}_stage_temp"

    cluster_additional_configs = {
        'spark.jars': '/opt/spark/third-party-jars/postgresql-42.2.24.jar'
    }

    spark = get_spark_session(mode=spark_mode, additional_configs=cluster_additional_configs)

     # Where the parquet files are contained
    partitioned_path = os.path.join(SPARK_PARTITION_FILES_DIR, f'{tripdata_type}/pq/stage/{year}-{month:02d}/year={year}/month={month}')
    stage_path = os.path.join(SPARK_PARTITION_FILES_DIR, f'{tripdata_type}/pq/stage/{year}-{month}')

    df = spark.read \
                .format("jdbc") \
                .option("url", "jdbc:postgresql://nyc-taxi-postgres:5432/nyc_taxi_data") \
                .option("dbtable", staging_temp_table) \
                .option("user", "postgres") \
                .option("password", "postgres") \
                .option("driver", "org.postgresql.Driver") \
                .load()

   
    # Add year and month columns for partitioning
    df = df.withColumn("year", pyspark_year(col("pickup_datetime"))) \
           .withColumn("month", pyspark_month(col("pickup_datetime")))

    # Write DataFrame to Parquet with partitioning
    df.write.partitionBy("year", "month").parquet(stage_path, mode="overwrite")

def read_parquet(args, kwargs):
    """Helper function to read parquet files based on the year and month."""
    year = kwargs['year']
    month = kwargs['month']
    spark_mode = kwargs['spark_mode']

    #Spark Partition parameters
    tripdata_type = kwargs['configuration'].get('tripdata_type')

    base_stage_path = os.path.join(SPARK_PARTITION_FILES_DIR, f'{tripdata_type}/pq/stage/{year}-{month:02d}')
    partition_path = os.path.join(base_stage_path, f'year={year}', f'month={month}')

    cluster_additional_configs = {
        'spark.jars': '/opt/spark/third-party-jars/postgresql-42.2.24.jar'
    }

    spark = get_spark_session(mode=spark_mode, additional_configs=cluster_additional_configs)

    yellow_tripdata_schema = YellowTripDataSchema(spark_session=spark)

    df = spark.read.schema(yellow_tripdata_schema.get_dataframe_schema(partition_path)).parquet(partition_path)

    return df

@test
def test_output(*args, **kwargs) -> None:
    """
    Template code for testing the output of the block.
    """
    year = kwargs['year']
    month = kwargs['month']

    df = read_parquet(args, kwargs)

    assert df.count() > 0, "DataFrame is empty"