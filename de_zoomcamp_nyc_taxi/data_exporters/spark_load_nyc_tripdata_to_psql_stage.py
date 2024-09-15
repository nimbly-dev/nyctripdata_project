from pyspark.sql import SparkSession
import os
from os import path
from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from de_zoomcamp_nyc_taxi.model.schema.yellow_tripdata import YellowTripDataSchema
from de_zoomcamp_nyc_taxi.utils.spark.spark_util import get_sparksession_for_codeblock
from pyspark.sql.functions import lit

SPARK_PARTITION_FILES_DIR = os.getenv('SPARK_PARTITION_FILES_DIR', '/opt/spark/spark-warehouse/partitioned')

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

@data_exporter
def export_data_to_postgres(data, *args, **kwargs) -> None:
    year = kwargs['year']
    month = kwargs['month']

    # Initialize Spark session
    spark = get_sparksession_for_codeblock(args, kwargs)

    # Path to partitioned parquet files
    partitioned_path = os.path.join(SPARK_PARTITION_FILES_DIR, f'yellow_tripdata/pq/stage/year={year}/month={month}')
    
    # Load DataFrame from parquet
    yellow_tripdata_schema = YellowTripDataSchema(spark_session=spark)
    df = spark.read.schema(yellow_tripdata_schema.get_dataframe_schema(partitioned_path)).parquet(partitioned_path)

    # Define PostgreSQL connection options
    schema_name = kwargs['configuration'].get('schema_name')
    table_name = kwargs['configuration'].get('table_name')
    actual_table_name = f"{schema_name}.{table_name}_{year}{month:02d}_stage_temp"

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
