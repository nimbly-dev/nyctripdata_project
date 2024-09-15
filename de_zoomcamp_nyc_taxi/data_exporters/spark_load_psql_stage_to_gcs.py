import os
from pyspark.sql.functions import col, date_format
from de_zoomcamp_nyc_taxi.model.schema.yellow_tripdata import YellowTripDataSchema
from de_zoomcamp_nyc_taxi.model.schema.green_tripdata import GreenTripDataSchema
from de_zoomcamp_nyc_taxi.utils.spark.spark_util import get_spark_session

from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.google_cloud_storage import GoogleCloudStorage
from os import path

# Maintain existing environment variables for directories
SPARK_PARTITION_FILES_DIR = os.getenv('SPARK_PARTITION_FILES_DIR', '/opt/spark/spark-warehouse/partitioned')
SPARK_WAREHOUSE_DIR = os.getenv('SPARK_WAREHOUSE_DIR', '/opt/spark/spark-warehouse')

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

@data_exporter
def export_data(df, *args, **kwargs):
    LOG = kwargs.get('logger')
    year = kwargs['year']
    month = kwargs['month']
    tripdata_type = kwargs.get('tripdata_type', 'default_table')
    spark_mode = kwargs.get('spark_mode', 'local')
    project_id = 'terraform-demo-424912'
    bucket_name = 'terraform-demo-424912-mage-zoompcamp-bucket'
    
    cluster_additional_configs = {
        'spark.jars': '/opt/spark/third-party-jars/gcs-connector-hadoop3-latest.jar',
        'spark.driver.extraClassPath': '/opt/spark/third-party-jars/gcs-connector-hadoop3-latest.jar',
        'spark.hadoop.fs.gs.project.id': 'terraform-demo-424912'
    }

    spark = get_spark_session(mode=spark_mode, additional_configs=cluster_additional_configs, appname='spark_load_psql_stage_to_gcs')
    base_stage_path = os.path.join(SPARK_PARTITION_FILES_DIR, f'{tripdata_type}/pq/stage/{year}-{month:02d}')
    partition_path = os.path.join(base_stage_path, f'year={year}', f'month={month}')

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

    # Add partition_date column for partitioning
    df_spark = df.withColumn("partition_date", date_format(col("pickup_datetime"), "yyyy-MM-dd"))


    root_path = f'gs://{bucket_name}/{tripdata_type}'
    month_path = f'{year}-{month:02d}'

    df_spark.write \
            .partitionBy("partition_date") \
            .mode('overwrite') \
            .parquet(os.path.join(root_path, month_path))

    LOG.info(f"Data successfully written to {root_path}")

    # Stop the Spark session
    spark.stop()


