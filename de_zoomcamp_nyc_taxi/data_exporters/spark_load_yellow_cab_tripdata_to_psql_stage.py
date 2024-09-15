from pyspark.sql import SparkSession
import os
from os import path
from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from de_zoomcamp_nyc_taxi.model.schema.yellow_tripdata import YellowTripDataSchema
from de_zoomcamp_nyc_taxi.utils.spark.spark_util import get_sparksession_for_codeblock, get_spark_session, print_spark_config
from pyspark.sql.functions import lit

SPARK_PARTITION_FILES_DIR = os.getenv('SPARK_PARTITION_FILES_DIR', '/opt/spark/spark-warehouse/partitioned')

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

@data_exporter
def export_data_to_postgres(data, *args, **kwargs) -> None:
    year = kwargs['year']
    month = kwargs['month']
    spark_mode = kwargs['spark_mode']
    LOG = kwargs.get('logger')

    # Initialize Spark session
    # spark = kwargs['spark']

    cluster_additional_configs = {
        'spark.jars': '/opt/spark/third-party-jars/postgresql-42.2.24.jar'
    }

    spark = get_spark_session(mode=spark_mode, additional_configs=cluster_additional_configs)

    # print_spark_config(spark)
    # Path to partitioned parquet files
    partitioned_path = os.path.join(SPARK_PARTITION_FILES_DIR, f'yellow_cab_tripdata/pq/stage/{year}-{month:02d}/year={year}/month={month}')

    # Load DataFrame from parquet
    yellow_tripdata_schema = YellowTripDataSchema(spark_session=spark)
    df = spark.read.schema(yellow_tripdata_schema.get_dataframe_schema(partitioned_path)).parquet(partitioned_path)

    # Define PostgreSQL connection options
    schema_name = kwargs['configuration'].get('schema_name')
    table_name = kwargs['configuration'].get('table_name')
    actual_table_name = f"{schema_name}.{table_name}_{year}{month}_stage_temp"

    # Write DataFrame to PostgreSQL
    df.write \
      .format("jdbc") \
      .option("url", "jdbc:postgresql://nyc-taxi-postgres:5432/nyc_taxi_data") \
      .option("dbtable", actual_table_name) \
      .option("user", "postgres") \
      .option("password", "postgres") \
      .option("driver", "org.postgresql.Driver") \
      .mode("overwrite") \
      .save()

    spark.stop()
