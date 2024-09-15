import pandas as pd
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
def export_yellow_taxi_data_to_bigquery(df: DataFrame, **kwargs) -> None:
    project_id = 'terraform-demo-424912'
    dataset_id = 'yellow_cab_dataset'
    table_id = 'yellow_cab_tripdata'
    view_id = 'yellow_cab_tripdata_view_staging'
    full_table_id = f'{project_id}.{dataset_id}.{table_id}'
    full_view_id = f'{project_id}.{dataset_id}.{view_id}'
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'


    bq = BigQuery.with_config(ConfigFileLoader(config_path, config_profile))
    client = bq.client

    # Load data into staging table
    temp_table_id = f'{dataset_id}.temp_table_for_view'
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        schema=[
            bigquery.SchemaField("dwid", "STRING"),
            bigquery.SchemaField("vendor_id", "INT64"),
            bigquery.SchemaField("pickup_datetime", "TIMESTAMP"),
            bigquery.SchemaField("dropoff_datetime", "TIMESTAMP"),
            bigquery.SchemaField("passenger_count", "INT64"),
            bigquery.SchemaField("trip_distance", "FLOAT64"),
            bigquery.SchemaField("ratecode_id", "INT64"),
            bigquery.SchemaField("store_and_fwd_flag", "STRING"),
            bigquery.SchemaField("pu_location_id", "INT64"),
            bigquery.SchemaField("do_location_id", "INT64"),
            bigquery.SchemaField("payment_type", "INT64"),
            bigquery.SchemaField("fare_amount", "FLOAT64"),
            bigquery.SchemaField("extra", "FLOAT64"),
            bigquery.SchemaField("mta_tax", "FLOAT64"),
            bigquery.SchemaField("tip_amount", "FLOAT64"),
            bigquery.SchemaField("tolls_amount", "FLOAT64"),
            bigquery.SchemaField("improvement_surcharge", "FLOAT64"),
            bigquery.SchemaField("total_amount", "FLOAT64"),
            bigquery.SchemaField("congestion_surcharge", "FLOAT64"),
            bigquery.SchemaField("airport_fee", "FLOAT64"),
        ],
    )
    load_job = client.load_table_from_dataframe(df, temp_table_id, job_config=job_config)
    load_job.result()  # Wait for the job to complete

    # Create or replace the view
    create_view_query = f"""
    CREATE OR REPLACE VIEW `{full_view_id}` AS
    SELECT * FROM `{temp_table_id}`
    """
    client.query(create_view_query).result()

    # Create the main table if it does not exist
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS `{full_table_id}` (
        vendor_id INT64,
        pickup_datetime TIMESTAMP,
        dropoff_datetime TIMESTAMP,
        passenger_count INT64,
        trip_distance FLOAT64,
        ratecode_id INT64,
        store_and_fwd_flag STRING,
        pu_location_id INT64,
        do_location_id INT64,
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
        dwid STRING
    )
    """
    client.query(create_table_query).result()

    # Merge data from staging table to main table
    merge_query = f"""
    MERGE `{full_table_id}` T
    USING `{full_view_id}` S
    ON T.dwid = S.dwid
    WHEN MATCHED THEN
      UPDATE SET
        vendor_id = S.vendor_id,
        pickup_datetime = S.pickup_datetime,
        dropoff_datetime = S.dropoff_datetime,
        passenger_count = S.passenger_count,
        trip_distance = S.trip_distance,
        ratecode_id = S.ratecode_id,
        store_and_fwd_flag = S.store_and_fwd_flag,
        pu_location_id = S.pu_location_id,
        do_location_id = S.do_location_id,
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
        vendor_id,
        pickup_datetime,
        dropoff_datetime,
        passenger_count,
        trip_distance,
        ratecode_id,
        store_and_fwd_flag,
        pu_location_id,
        do_location_id,
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
        S.vendor_id,
        S.pickup_datetime,
        S.dropoff_datetime,
        S.passenger_count,
        S.trip_distance,
        S.ratecode_id,
        S.store_and_fwd_flag,
        S.pu_location_id,
        S.do_location_id,
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

