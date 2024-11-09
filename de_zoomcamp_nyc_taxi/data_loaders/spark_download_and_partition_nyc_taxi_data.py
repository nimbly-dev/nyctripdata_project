import os
import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import year as pyspark_year, month as pyspark_month
from de_zoomcamp_nyc_taxi.utils.spark.spark_util import get_spark_session
from de_zoomcamp_nyc_taxi.utils.common.common_util import extract_year_month
import calendar
from datetime import datetime

SPARK_LAKEHOUSE_DIR = os.getenv('SPARK_LAKEHOUSR_DIR', '/opt/spark/spark-lakehouse')
USER_AGENT_HEADER = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data(*args, **kwargs):
    LOG = kwargs.get('logger')
    pipeline_run_name = kwargs['pipeline_run_name']
    tripdata_type = kwargs['tripdata_type']
    base_url = kwargs['configuration'].get('base_url')
    partition_column = kwargs['configuration'].get('partition_column')
    dev_limit_rows = kwargs.get('dev_limit_rows', 0)
    
    year_month = kwargs['year_month']
    year, month = extract_year_month(year_month)['year'], extract_year_month(year_month)['month']
    
    spark = get_spark_session(
        mode='cluster',
        appname=f'{pipeline_run_name}_{tripdata_type}_spark_download_and_partition_nyc_taxi_data'
    )

    download_url = f"{base_url}{year}-{month:02d}.parquet"
    download_dir = f"{SPARK_LAKEHOUSE_DIR}/downloads/{tripdata_type}"
    download_path = os.path.join(download_dir, f"{year}-{month:02d}.parquet")

    os.makedirs(download_dir, exist_ok=True)

    LOG.info(f'Download: {download_url}')
    response = requests.get(download_url, headers=USER_AGENT_HEADER)
    if response.status_code != 200:
        LOG.error(f"Failed to download data for {year}-{month:02d}: {response.status_code}")
        raise Exception(f"Download failed for {year}-{month:02d} with status code {response.status_code}")

    with open(download_path, 'wb') as f:
        f.write(response.content)
    LOG.info(f"Data downloaded to {download_path}")

    df = spark.read.parquet(download_path)
    df = df.filter(
        (pyspark_year(df[partition_column]) == year) &
        (pyspark_month(df[partition_column]) == month)
    )

    if dev_limit_rows > 0:
        fraction = dev_limit_rows / df.count()
        df = df.sample(withReplacement=False, fraction=fraction)
        LOG.info(f"Sampled approximately {dev_limit_rows} rows from the DataFrame")

    output_path = f'{SPARK_LAKEHOUSE_DIR}/partitioned/{tripdata_type}/tmp/pq/raw/{pipeline_run_name}'
    os.makedirs(output_path, exist_ok=True)

    df = df.withColumn("year", pyspark_year(df[partition_column])) \
           .withColumn("month", pyspark_month(df[partition_column]))

    df.write.partitionBy("year", "month").parquet(output_path, mode="append")
    LOG.info(f"Data written to {output_path}")

    spark.stop()
    return output_path


@test
def validate_data(output, *args, **kwargs):
    LOG = kwargs.get('logger')
    base_path = output.strip()

    if not base_path or not os.path.exists(base_path):
        LOG.error(f"Invalid or non-existent base path: {base_path}")
        assert False, f"Invalid or non-existent base path: {base_path}"

    LOG.info(f"Validating base path: {base_path}")

    year_month_paths = [
        os.path.join(base_path, d) for d in os.listdir(base_path)
        if os.path.isdir(os.path.join(base_path, d)) and d.startswith('year=')
    ]

    if not year_month_paths:
        LOG.error(f"No 'year' directories found in: {base_path}")
        assert False, f"No 'year' directories found in: {base_path}"

    for year_path in year_month_paths:
        month_paths = [
            os.path.join(year_path, d) for d in os.listdir(year_path)
            if os.path.isdir(os.path.join(year_path, d)) and d.startswith('month=')
        ]

        if not month_paths:
            LOG.error(f"No 'month' directories found in: {year_path}")
            assert False, f"No 'month' directories found in: {year_path}"

        for month_path in month_paths:
            LOG.info(f"Validating data at path: {month_path}")
            parquet_files = [f for f in os.listdir(month_path) if f.endswith('.parquet')]
            if parquet_files:
                LOG.info(f"Found parquet files: {parquet_files} in directory: {month_path}")
            else:
                LOG.error(f"No parquet files found in the directory: {month_path}")
                assert False, f"No parquet files found in the directory: {month_path}"

    LOG.info("Validation complete")


