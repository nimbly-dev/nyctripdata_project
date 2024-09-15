from pyspark.sql import SparkSession
import os
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from de_zoomcamp_nyc_taxi.model.schema.yellow_tripdata import YellowTripDataSchema
from de_zoomcamp_nyc_taxi.utils.spark.spark_util import get_spark_session
from de_zoomcamp_nyc_taxi.utils.sql.sql_util import run_sql_on_postgres
from pyspark.sql.functions import lit

SPARK_PARTITION_FILES_DIR = os.getenv('SPARK_PARTITION_FILES_DIR', '/opt/spark/spark-warehouse/partitioned')

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

@data_exporter
def export_data_to_postgres(data, *args, **kwargs) -> None:
    year = kwargs['year']
    month = kwargs['month']
    spark_mode = kwargs['spark_mode']

    tripdata_type = kwargs['configuration'].get('tripdata_type')

    cluster_additional_configs = {
        'spark.jars': '/opt/spark/third-party-jars/postgresql-42.2.24.jar'
    }

    spark = get_spark_session(mode=spark_mode, additional_configs=cluster_additional_configs)

    # Path to partitioned parquet files
    partitioned_path = os.path.join(SPARK_PARTITION_FILES_DIR, f'{tripdata_type}/pq/stage/{year}-{month:02d}/year={year}/month={month}')


    yellow_tripdata_schema = YellowTripDataSchema(spark_session=spark)
    df = spark.read.schema(yellow_tripdata_schema.get_dataframe_schema(partitioned_path)).parquet(partitioned_path)


    schema_name = kwargs['configuration'].get('schema_name')
    table_name = f"{tripdata_type}_{year}_{month}_stage_temp"
    engine_name = f'postgresql://postgres:postgres@nyc-taxi-postgres:5432/nyc_taxi_data'
    drop_table_sql = f"DROP TABLE IF EXISTS {table_name};"
    run_sql_on_postgres(drop_table_sql,engine_name)

  
    df.write \
      .format("jdbc") \
      .option("url", "jdbc:postgresql://nyc-taxi-postgres:5432/nyc_taxi_data") \
      .option("dbtable", table_name) \
      .option("user", "postgres") \
      .option("password", "postgres") \
      .option("driver", "org.postgresql.Driver") \
      .mode("overwrite") \
      .save()


    alter_table_sql = f"""
    ALTER TABLE {table_name}
    ADD CONSTRAINT {table_name}_pkey PRIMARY KEY (dwid);
    """
    run_sql_on_postgres(alter_table_sql,engine_name)
