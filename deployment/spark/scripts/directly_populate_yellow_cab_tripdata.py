import os
import requests
from pyspark.sql.functions import year as pyspark_year, month as pyspark_month
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import types as T
from pyspark.sql.functions import col, concat_ws, sha2
import pyspark.sql.functions as F  # To alias other functions as F
import shutil
import calendar
from datetime import datetime, timedelta
import concurrent.futures
import glob
import psycopg2
import logging
from typing import Any

# Initialize logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
start_year = 2021
start_month = 1
end_year = 2022
end_month = 12
master_url = 'spark://spark-master:7077'
SPARK_LAKEHOUSE_DIR = '/opt/spark/spark-lakehouse'
USER_AGENT_HEADER = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}
connection_string = os.getenv('POSTGRES_CONNECTION_STRING', "dbname=nyc_taxi_staging_postgres user=staging-service-account@de-nyctripdata-project.iam.com password=password123! host=postgres-staging port=5433")
target_table = 'yellow_cab_tripdata_staging'
base_csv_temp_path = os.path.join(SPARK_LAKEHOUSE_DIR, 'yellow_cab_tripdata/tmp/csv/directly_populate_yellow_cab_tripdata')

# SCHEMA CLASS
class YellowTripDataSchema:
    def __init__(self, spark_session: SparkSession):
        self.spark = spark_session

    def get_base_schema(self) -> T.StructType:
        return T.StructType([
            T.StructField("VendorID", T.IntegerType(), nullable=True),
            T.StructField("tpep_pickup_datetime", T.TimestampType(), nullable=False),
            T.StructField("tpep_dropoff_datetime", T.TimestampType(), nullable=False),
            T.StructField("passenger_count", T.IntegerType(), nullable=True),
            T.StructField("trip_distance", T.FloatType(), nullable=True),
            T.StructField("RatecodeID", T.IntegerType(), nullable=True),
            T.StructField("store_and_fwd_flag", T.StringType(), nullable=True),
            T.StructField("PULocationID", T.IntegerType(), nullable=True),
            T.StructField("DOLocationID", T.IntegerType(), nullable=True),
            T.StructField("payment_type", T.IntegerType(), nullable=True),
            T.StructField("fare_amount", T.FloatType(), nullable=True),
            T.StructField("extra", T.FloatType(), nullable=True),
            T.StructField("mta_tax", T.FloatType(), nullable=True),
            T.StructField("tip_amount", T.FloatType(), nullable=True),
            T.StructField("tolls_amount", T.FloatType(), nullable=True),
            T.StructField("improvement_surcharge", T.FloatType(), nullable=True),
            T.StructField("total_amount", T.FloatType(), nullable=True),
            T.StructField("congestion_surcharge", T.FloatType(), nullable=True),
            T.StructField("airport_fee", T.FloatType(), nullable=True),
        ])

    def cast_columns(self, df: DataFrame) -> DataFrame:
        schema = self.get_base_schema()
        for field in schema.fields:
            if field.name in df.columns:
                df = df.withColumn(field.name, col(field.name).cast(field.dataType))
        return df

# Utility methods for PostgreSQL Copy
def copy_csv_file_to_postgres(csv_file: str, connection_string: str, partition_table_name: str) -> None:
    try:
        with psycopg2.connect(connection_string) as conn:
            with conn.cursor() as cur:
                with open(csv_file, 'r') as f:
                    copy_command = f"COPY {partition_table_name} FROM STDIN WITH CSV HEADER"
                    cur.copy_expert(copy_command, f)
                conn.commit()
    except Exception as e:
        logger.error(f"Error processing file {csv_file}: {e}")
        raise

def copy_csv_files_parallel(connection_string: str, partition_table_name: str, csv_dir: str) -> None:
    csv_files = glob.glob(os.path.join(csv_dir, '*.csv'))
    if not csv_files:
        raise FileNotFoundError(f"No CSV files found in {csv_dir}")

    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = [executor.submit(copy_csv_file_to_postgres, csv_file, connection_string, partition_table_name) for csv_file in csv_files]
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logger.error(f"Exception occurred during parallel copy: {e}")
                raise

# Create spark session here
spark = SparkSession.builder \
    .appName('directly_populate_yellow_cab_tripdata') \
    .master(master_url) \
    .config('spark.executor.cores', '2') \
    .config('spark.executor.memory', '3g') \
    .config('spark.driver.memory', '2g') \
    .config('spark.default.parallelism', '6') \
    .config('spark.jars', '/opt/spark/third-party-jars/postgresql-42.2.24.jar') \
    .config('spark.driver.extraClassPath', '/opt/spark/third-party-jars/postgresql-42.2.24.jar') \
    .getOrCreate()

current_date = datetime(start_year, start_month, 1)
end_date = datetime(end_year, end_month, calendar.monthrange(end_year, end_month)[1])
output_path = f'{SPARK_LAKEHOUSE_DIR}/partitioned/yellow_cab_tripdata/tmp/pq/direct/raw/'
base_url = 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_'

while current_date <= end_date:
    year = current_date.year
    month = current_date.month

    download_url = f"{base_url}{year}-{month:02d}.parquet"
    download_dir = f"{SPARK_LAKEHOUSE_DIR}/downloads/yellow_cab_tripdata"
    download_path = os.path.join(download_dir, f"{year}-{month:02d}.parquet")

    os.makedirs(download_dir, exist_ok=True)

    response = requests.get(download_url, headers=USER_AGENT_HEADER)
    if response.status_code != 200:
        raise Exception(f"Download failed for {year}-{month:02d} with status code {response.status_code}")

    with open(download_path, 'wb') as f:
        f.write(response.content)

    df = spark.read.parquet(download_path)

    # Transform and Clean Data
    df = df.withColumnRenamed("PULocationID", "pu_location_id") \
           .withColumnRenamed("DOLocationID", "do_location_id") \
           .withColumnRenamed("tpep_pickup_datetime", "pickup_datetime") \
           .withColumnRenamed("tpep_dropoff_datetime", "dropoff_datetime") \
           .withColumnRenamed("RatecodeID", "ratecode_id") \
           .withColumnRenamed("VendorID", "vendor_id")

    df = (
        df.dropDuplicates()
        .dropna(subset=["pu_location_id", "do_location_id"])
        .filter((F.col('trip_distance') > 0) & (F.col('trip_distance') <= 10000))
        .filter((F.col('fare_amount') > 0) & (F.col('fare_amount') <= 100000))
        .filter((F.col('trip_distance') > 0) & (F.col('passenger_count') > 0))
        .filter(F.col('pickup_datetime') != F.col('dropoff_datetime'))
        .withColumn('store_and_fwd_flag', F.col('store_and_fwd_flag').substr(0, 1))
    )

    # Add unique key and dwid
    df = df.withColumn('unique_key', concat_ws('_',
        col('pickup_datetime').cast("string"),
        col('dropoff_datetime').cast("string"),
        col('pu_location_id').cast("string"),
        col('do_location_id').cast("string")
    ))
    df = df.withColumn('dwid', sha2(col('unique_key'), 256).substr(1, 16)).drop('unique_key')

    # Write CSV to temp directory
    temp_csv_dir = os.path.join(base_csv_temp_path, f'year={year}', f'month={month}')
    df.write.csv(temp_csv_dir, header=True, mode="overwrite")

    # Load data into PostgreSQL
    try:
        with psycopg2.connect(connection_string) as conn:
            with conn.cursor() as cur:
                target_date = f"{year}-{month:02d}-01"
                partition_query = f"SELECT public.create_partition_if_not_exists('yellow_cab_tripdata_production', '{target_date}');"
                cur.execute(partition_query)
                conn.commit()

                partition_table_name = f'yellow_cab_tripdata_staging_{year}_{month:02d}'
                cur.execute(f'TRUNCATE TABLE {partition_table_name};')
                conn.commit()

                copy_csv_files_parallel(connection_string, target_table, temp_csv_dir)
    except psycopg2.Error as e:
        logger.error(f"Error while loading to production SQL: {e}")

    # Cleanup CSV
    shutil.rmtree(temp_csv_dir)

    # Increment month
    if month == 12:
        year += 1
        month = 1
    else:
        month += 1
    current_date = datetime(year, month, 1)


# spark-submit \
#   --master spark://spark-master:7077 \
#   /path/to/your_script.py
