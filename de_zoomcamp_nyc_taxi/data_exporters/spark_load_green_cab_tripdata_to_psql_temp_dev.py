from pyspark.sql import SparkSession
import os
from os import path
from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from de_zoomcamp_nyc_taxi.model.schema.green_tripdata import GreenTripDataSchema
from de_zoomcamp_nyc_taxi.utils.spark.spark_util import get_sparksession_for_codeblock, get_spark_session, print_spark_config
from pyspark.sql.functions import lit

SPARK_PARTITION_FILES_DIR = os.getenv('SPARK_PARTITION_FILES_DIR', '/opt/spark/spark-warehouse/partitioned')

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

@data_exporter
def export_data_to_postgres(data, *args, **kwargs) -> None:
    year = kwargs.get('year')
    month = kwargs.get('month')
    spark_mode = kwargs.get('spark_mode', 'local')
    database_url = kwargs['configuration'].get('database_url')
    db_name = kwargs['configuration'].get('db_name')
    schema_name = kwargs['configuration'].get('schema_name')
    table_name = kwargs['configuration'].get('table_name')
    LOG = kwargs.get('logger')

    if not year or not month:
        raise ValueError("Error: 'year' and 'month' must be provided.")
    if not database_url or not db_name or not schema_name or not table_name:
        raise ValueError("Error: 'database_url', 'db_name', 'schema_name', and 'table_name' must be provided.")

    additional_configs = {
        'spark.jars': '/opt/spark/third-party-jars/postgresql-42.2.24.jar',
        'spark.driver.extraClassPath': '/opt/spark/third-party-jars/postgresql-42.2.24.jar'
    }

    try:
        spark = get_spark_session(mode=spark_mode, additional_configs=additional_configs, appname='spark_load_yellow_cab_tripdata_to_psql_temp_dev')
        LOG.info("Spark session initiated.")

        partitioned_path = os.path.join(SPARK_PARTITION_FILES_DIR, f'green_cab_tripdata/pq/dev/{year}-{month:02d}/year={year}/month={month}')

        green_tripdata_schema = GreenTripDataSchema(spark_session=spark)

        inferred_schema = green_tripdata_schema.get_dataframe_schema(partitioned_path)

        # Apply the inferred schema to read the Parquet files
        df = spark.read.schema(inferred_schema).parquet(partitioned_path)

        actual_table_name = f"{schema_name}.{table_name}_{year}{month:02d}_dev_temp"

        df.write \
          .format("jdbc") \
          .option("url", f'jdbc:postgresql://{database_url}/{db_name}') \
          .option("dbtable", actual_table_name) \
          .option("user", "postgres") \
          .option("password", "postgres") \
          .option("driver", "org.postgresql.Driver") \
          .mode("overwrite") \
          .save()

        LOG.info(f"Data written to PostgreSQL table: {actual_table_name}")
    
    except Exception as e:
        LOG.error(f"Error exporting data to PostgreSQL: {e}", exc_info=True)
    
    finally:
        spark.stop()
