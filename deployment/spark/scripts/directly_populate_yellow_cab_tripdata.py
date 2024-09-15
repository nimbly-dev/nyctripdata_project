from pyspark.sql import SparkSession


#Configuration
start_year = 2021
start_month = 1
end_year = 1
end_month = 2022
master_url = 'spark://spark-master:7077';
builder = SparkSession.builder \
    .appName("directly_populate_yellow_cab_tripdata") \
    .master(master_url) \
    .config('spark.executor.cores', '2') \
    .config('spark.executor.memory', '3g') \
    .config('spark.driver.memory', '2g') \
    .config('spark.default.parallelism', '6')  


#DOWNLOAD THE PIPELINE 
start_date = datetime(start_year, start_month, 1)
last_day = calendar.monthrange(end_year, end_month)[1]
end_date = datetime(end_year, end_month, last_day)

current_date = start_date
output_path = f'{SPARK_LAKEHOUSE_DIR}/partitioned/yellow_cab_tripdata/tmp/pq/direct/raw/'
while current_date <= end_date:
    year = current_date.year
    month = current_date.month

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

    df = df.withColumn("year", pyspark_year(df[partition_column])) \
            .withColumn("month", pyspark_month(df[partition_column]))

    df.write.partitionBy("year", "month").parquet(output_path, mode="append")
    LOG.info(f"Data written to {output_path}")

    current_date = datetime(year + (month // 12), (month % 12) + 1, 1)