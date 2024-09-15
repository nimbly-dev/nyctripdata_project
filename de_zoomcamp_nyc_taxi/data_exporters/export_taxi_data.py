from mage_ai.settings.repo import get_repo_path
from mage_ai.io.bigquery import BigQuery
from mage_ai.io.config import ConfigFileLoader
from pandas import DataFrame
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from os import path

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

@data_exporter
def export_data_to_big_query(df: DataFrame, **kwargs) -> None:
    """
    Template for exporting data to a BigQuery warehouse with upsert functionality.
    Specify your configuration settings in 'io_config.yaml'.

    Docs: https://docs.mage.ai/design/data-loading#bigquery
    """
    project_id = 'terraform-demo-424912'
    dataset_id = 'nyc_taxi_data'
    table_id = 'yellow_cab_data'
    staging_table_id = 'yellow_cab_data_staging'
    full_table_id = f'{project_id}.{dataset_id}.{table_id}'
    full_staging_table_id = f'{project_id}.{dataset_id}.{staging_table_id}'
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    bq = BigQuery.with_config(ConfigFileLoader(config_path, config_profile))
    client = bq.client

    # Convert datetime columns to timestamp
    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])

    # Print the name and type of each column
    for column in df.columns:
        print(f'Column: {column}, Type: {df[column].dtype}')

    # Load data into staging table
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
    )
    load_job = client.load_table_from_dataframe(df, full_staging_table_id, job_config=job_config)
    load_job.result()  # Wait for the job to complete

    # Create the main table if it does not exist
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS `{full_table_id}` (
        vendorid INT64,
        tpep_pickup_datetime TIMESTAMP,
        tpep_dropoff_datetime TIMESTAMP,
        passenger_count INT64,
        trip_distance FLOAT64,
        ratecodeid INT64,
        store_and_fwd_flag STRING,
        pulocationid INT64,
        dolocationid INT64,
        payment_type INT64,
        fare_amount FLOAT64,
        extra FLOAT64,
        mta_tax FLOAT64,
        tip_amount FLOAT64,
        tolls_amount FLOAT64,
        improvement_surcharge FLOAT64,
        total_amount FLOAT64,
        congestion_surcharge FLOAT64,
        airport_fee FLOAT64,
        dwid STRING,
        PRIMARY KEY (dwid)
    )
    """
    client.query(create_table_query).result()

    # Merge data from staging table to main table
    merge_query = f"""
    MERGE `{full_table_id}` T
    USING `{full_staging_table_id}` S
    ON T.dwid = S.dwid
    WHEN MATCHED THEN
      UPDATE SET
        vendorid = S.vendorid,
        tpep_pickup_datetime = S.tpep_pickup_datetime,
        tpep_dropoff_datetime = S.tpep_dropoff_datetime,
        passenger_count = S.passenger_count,
        trip_distance = S.trip_distance,
        ratecodeid = S.ratecodeid,
        store_and_fwd_flag = S.store_and_fwd_flag,
        pulocationid = S.pulocationid,
        dolocationid = S.dolocationid,
        payment_type = S.payment_type,
        fare_amount = S.fare_amount,
        extra = S.extra,
        mta_tax = S.mta_tax,
        tip_amount = S.tip_amount,
        tolls_amount = S.tolls_amount,
        improvement_surcharge = S.improvement_surcharge,
        total_amount = S.total_amount,
        congestion_surcharge = S.congestion_surcharge,
        airport_fee = S.airport_fee
    WHEN NOT MATCHED THEN
      INSERT (
        vendorid,
        tpep_pickup_datetime,
        tpep_dropoff_datetime,
        passenger_count,
        trip_distance,
        ratecodeid,
        store_and_fwd_flag,
        pulocationid,
        dolocationid,
        payment_type,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        total_amount,
        congestion_surcharge,
        airport_fee,
        dwid
      )
      VALUES (
        S.vendorid,
        S.tpep_pickup_datetime,
        S.tpep_dropoff_datetime,
        S.passenger_count,
        S.trip_distance,
        S.ratecodeid,
        S.store_and_fwd_flag,
        S.pulocationid,
        S.dolocationid,
        S.payment_type,
        S.fare_amount,
        S.extra,
        S.mta_tax,
        S.tip_amount,
        S.tolls_amount,
        S.improvement_surcharge,
        S.total_amount,
        S.congestion_surcharge,
        S.airport_fee,
        S.dwid
      )
    """
    client.query(merge_query).result()
