from pyspark.sql import SparkSession
import os
from os import path
from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from de_zoomcamp_nyc_taxi.model.schema.yellow_tripdata import YellowTripDataSchema
from de_zoomcamp_nyc_taxi.utils.spark.spark_util import get_sparksession_for_codeblock

SPARK_PARTITION_FILES_DIR = os.getenv('SPARK_PARTITION_FILES_DIR', '/opt/spark/spark-warehouse/partitioned')

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

SPARK_PARTITION_FILES_DIR = os.getenv('SPARK_PARTITION_FILES_DIR', '/opt/spark/spark-warehouse/partitioned')
SPARK_WAREHOUSE_DIR = os.getenv('SPARK_WAREHOUSE_DIR', '/opt/spark/spark-warehouse')

@data_loader
def load_data_from_postgres(*args, **kwargs):
    year = kwargs['year']
    month = kwargs['month']
    partition_path = os.path.join(SPARK_PARTITION_FILES_DIR, f'yellow_tripdata/pq/stage')
    spark = get_sparksession_for_codeblock(args, kwargs)

    # PostgreSQL connection parameters
    schema_name = kwargs['configuration'].get('schema_name')
    table_name = kwargs['configuration'].get('table_name')
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = kwargs['configuration'].get('config_profile')
    staging_table_name = f"{schema_name}.{table_name}_staging"

    # Query to select data from staging table
    query = f"SELECT * \
                FROM {staging_table_name} \
               WHERE EXTRACT(YEAR FROM tpep_pickup_datetime) = {year} AND \
                     EXTRACT(MONTH FROM tpep_pickup_datetime) = {month}"

    with Postgres.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
        data = loader.load(query)

    df = spark.createDataFrame(data)
    df.printSchema()


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'