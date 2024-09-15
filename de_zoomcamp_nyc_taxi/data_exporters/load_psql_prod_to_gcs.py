import os
import logging
from pyspark.sql.functions import col, date_format
from mage_ai.data_preparation.decorators import data_exporter
from de_zoomcamp_nyc_taxi.model.schema.yellow_tripdata import YellowTripDataSchema
from de_zoomcamp_nyc_taxi.model.schema.green_tripdata import GreenTripDataSchema
from de_zoomcamp_nyc_taxi.utils.spark.spark_util import get_spark_session

# Maintain existing environment variables for directories
SPARK_PARTITION_FILES_DIR = os.getenv('SPARK_PARTITION_FILES_DIR', '/opt/spark/spark-warehouse/partitioned')
SPARK_WAREHOUSE_DIR = os.getenv('SPARK_WAREHOUSE_DIR', '/opt/spark/spark-warehouse')

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter
# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Set up Google Cloud credentials
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = './keys/my-creds.json'
bucket_name = 'terraform-demo-424912-mage-zoompcamp-bucket'
os.environ['GCP_PROJECT_ID'] = 'terraform-demo-424912'

@data_exporter
def export_data(df, *args, **kwargs) -> None:
    year = kwargs['year']
    month = kwargs['month']
    tripdata_type = kwargs.get('tripdata_type', 'default_table')
    spark_mode = kwargs.get('spark_mode', 'local')
    
    # Use the enhanced get_spark_session method with GCS configurations
    cluster_additional_configs = {
        'spark.jars': '/opt/spark/third-party-jars/gcs-connector-hadoop3-latest.jar,/opt/spark/third-party-jars/postgresql-42.2.24.jar',
        'spark.hadoop.fs.gs.impl': 'com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem',
        'spark.hadoop.google.cloud.auth.service.account.enable': 'true',
        'spark.hadoop.google.cloud.auth.service.account.json.keyfile': os.getenv('GOOGLE_APPLICATION_CREDENTIALS'),
        'spark.hadoop.fs.gs.project.id': os.getenv('GCP_PROJECT_ID')
    }

    spark = get_spark_session(mode=spark_mode, additional_configs=cluster_additional_configs, appname='spark_export_to_gcs')
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

    df = spark.read.schema(inferred_schema).parquet(partition_path)

    # Add partitioned_date column for partitioning
    df_spark = df.withColumn("partitioned_date", date_format(col("pickup_datetime"), "yyyy-MM-dd"))

    # Define the GCS root path
    root_path = f'gs://{bucket_name}/{tripdata_type}'

    # Write the DataFrame to GCS in Parquet format, partitioned by partitioned_date
    df_spark.write \
            .partitionBy("partitioned_date") \
            .mode('overwrite') \
            .parquet(root_path)

    logger.info(f"Data successfully written to {root_path}")

    # Stop the Spark session
    spark.stop()
