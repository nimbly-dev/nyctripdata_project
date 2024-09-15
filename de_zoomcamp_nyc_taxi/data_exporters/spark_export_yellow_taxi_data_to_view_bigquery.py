if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

from de_zoomcamp_nyc_taxi.model.schema.yellow_tripdata import YellowTripDataSchema
from de_zoomcamp_nyc_taxi.model.schema.green_tripdata import GreenTripDataSchema
from de_zoomcamp_nyc_taxi.utils.spark.spark_util import get_spark_session
from mage_ai.settings.repo import get_repo_path
from mage_ai.io.bigquery import BigQuery
from mage_ai.io.config import ConfigFileLoader
from pandas import DataFrame
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import os
from os import path

SPARK_PARTITION_FILES_DIR = os.getenv('SPARK_PARTITION_FILES_DIR', '/opt/spark/spark-warehouse/partitioned')
SPARK_WAREHOUSE_DIR = os.getenv('SPARK_WAREHOUSE_DIR', '/opt/spark/spark-warehouse')

@data_exporter
def export_data(data, *args, **kwargs):
    LOG = kwargs.get('logger')
    year = kwargs['year']
    month = kwargs['month']
    spark_mode = kwargs.get('spark_mode')
    tripdata_type = kwargs['tripdata_type']
    project_id = 'terraform-demo-424912'
    dataset_id = f'{project_id}.yellow_cab_dataset'
    table_id = 'yellow_cab_tripdata'
    view_id = 'yellow_cab_tripdata_{year}_{month:02d}_view_staging'
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    cluster_additional_configs = {
        'spark.jars': '/opt/spark/third-party-jars/spark-3.5-bigquery-0.40.0.jar',
        'spark.driver.extraClassPath': '/opt/spark/third-party-jars/spark-3.5-bigquery-0.40.0.jar'
    }

    partition_path = os.path.join(SPARK_PARTITION_FILES_DIR, f'{tripdata_type}/pq/stage/{year}-{month:02d}/year={year}/month={month}')
    
    spark = get_spark_session(mode=spark_mode, additional_configs=cluster_additional_configs, appname='spark_get_tripdata_from_stage_psql')
    LOG.info('Spark Session initiated')

    if tripdata_type == 'yellow_cab_tripdata':
        yellow_tripdata_schema = YellowTripDataSchema(spark_session=spark)
        inferred_schema = yellow_tripdata_schema.get_dataframe_schema(partition_path)
    elif tripdata_type == 'green_cab_tripdata':
        green_tripdata_schema = GreenTripDataSchema(spark_session=spark)
        inferred_schema = green_tripdata_schema.get_dataframe_schema(partition_path)
    else:
        raise ValueError(f"Unknown tripdata_type: {tripdata_type}")

    # Read the parquet data with the inferred schema
    df = spark.read.schema(inferred_schema).parquet(partition_path)


    # Define BigQuery configuration
    bq_temp_table_id = f'{dataset_id}.temp_table_for_{year}_{month:02d}_view'
    
    df.write \
        .format("bigquery") \
        .option("writeMethod", "direct") \
        .option("project", "terraform-demo-424912") \
        .option("parentProject","terraform-demo-424912") \
        .option("credentialsFile","/opt/spark/keys/my-creds.json") \
        .mode('overwrite') \
        .save(f'{bq_temp_table_id}')


    # Create or replace the view
    # create_view_query = f"""
    # CREATE OR REPLACE VIEW `{view_id}` AS
    # SELECT * FROM `{bq_temp_table_id}`
    # """
    # bq = BigQuery.with_config(ConfigFileLoader(config_path, config_profile))
    # client = bq.client
    # bq_client.query(create_view_query).result()

    # LOG.info(f"Data successfully written to BigQuery view: {full_view_id}")

    # Stop the Spark session
    spark.stop()

