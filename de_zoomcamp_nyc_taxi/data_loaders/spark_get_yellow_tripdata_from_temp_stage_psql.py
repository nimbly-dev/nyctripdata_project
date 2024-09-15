from pyspark.sql import SparkSession
import os
from os import path
from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from pyspark.sql.functions import col, year as pyspark_year, month as pyspark_month
from de_zoomcamp_nyc_taxi.model.schema.yellow_tripdata import YellowTripDataSchema
from de_zoomcamp_nyc_taxi.utils.spark.spark_util import get_sparksession_for_codeblock
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

    # PostgreSQL connection parameters
    schema_name = kwargs['configuration'].get('schema_name')
    table_name = kwargs['configuration'].get('table_name')
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = kwargs['configuration'].get('config_profile')
    staging_temp_table = f"{schema_name}.{table_name}_{year}{month:02d}_stage_temp"

    #Spark Partition parameters
    partition_name = kwargs['configuration'].get('partition_name')


    spark = get_sparksession_for_codeblock(args, kwargs)

     # Where the parquet files are contained
    partitioned_path = os.path.join(SPARK_PARTITION_FILES_DIR, f'{partition_name}/pq/stage/year={year}/month={month}')
    stage_path = os.path.join(SPARK_PARTITION_FILES_DIR, f'{partition_name}/pq/stage')

    df = spark.read \
                .format("jdbc") \
                .option("url", "jdbc:postgresql://nyc-taxi-postgres:5432/nyc_taxi_data") \
                .option("dbtable", staging_temp_table) \
                .option("user", "postgres") \
                .option("password", "postgres") \
                .option("driver", "org.postgresql.Driver") \
                .load()

   
    # Add year and month columns for partitioning
    df = df.withColumn("year", pyspark_year(col("tpep_pickup_datetime"))) \
           .withColumn("month", pyspark_month(col("tpep_pickup_datetime")))

    # Write DataFrame to Parquet with partitioning
    df.write.partitionBy("year", "month").parquet(stage_path, mode="overwrite")

def read_parquet(args, kwargs):
    """Helper function to read parquet files based on the year and month."""
    year = kwargs['year']
    month = kwargs['month']

    base_stage_path = os.path.join(SPARK_PARTITION_FILES_DIR, f'{partition_name}/pq/stage')
    partition_path = os.path.join(base_stage_path, f'year={year}', f'month={month}')

    spark = get_sparksession_for_codeblock(args, kwargs)
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
    spark = get_sparksession_for_codeblock(args, kwargs)

    df = read_parquet(args, kwargs)

    assert df.count() > 0, "DataFrame is empty"