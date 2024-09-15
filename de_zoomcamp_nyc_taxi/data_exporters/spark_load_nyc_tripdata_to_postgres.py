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

# def run_sql_on_postgres(sql_query):
#     from sqlalchemy import create_engine
#     engine = create_engine('postgresql://postgres:postgres@nyc-taxi-postgres:5432/nyc_taxi_data')
#     with engine.connect() as connection:
#         connection.execute(sql_query)

# def add_partition(year, month):
#     start_date = f"{year}-{month:02d}-01 00:00:00"
#     end_date = f"{year}-{month+1:02d}-01 00:00:00" if month < 12 else f"{year+1}-01-01 00:00:00"
    
#     add_partition_sql = f"""
#     CREATE TABLE IF NOT EXISTS yellow_cab_tripdata_staging_{year}_{month:02d}
#     PARTITION OF yellow_cab_tripdata_staging
#     FOR VALUES FROM (TIMESTAMP '{start_date}') TO (TIMESTAMP '{end_date}');
#     """
#     run_sql_on_postgres(add_partition_sql)

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

    # Add a new column 'dwid' with an empty string
    # df = df.withColumn("dwid", lit(""))

    # Add partition for the specific year and month
    # add_partition(year, month)

    # Define PostgreSQL connection options
    schema_name = kwargs['configuration'].get('schema_name')
    table_name = kwargs['configuration'].get('table_name')
    staging_viewtemp_table = f"{schema_name}.{table_name}_{year}{month}_stage_temp"

    # Write DataFrame to PostgreSQL
    df.write \
      .format("jdbc") \
      .option("url", "jdbc:postgresql://nyc-taxi-postgres:5432/nyc_taxi_data") \
      .option("dbtable", staging_viewtemp_table) \
      .option("user", "postgres") \
      .option("password", "postgres") \
      .option("driver", "org.postgresql.Driver") \
      .mode("overwrite") \
      .save()
