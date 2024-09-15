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

# Set the data types for each column
dtype_map = {
    'dispatching_base_num': 'object',
    'fhpickup_datetime': 'datetime64[ns]',
    'fhdropoff_datetime': 'datetime64[ns]',
    'pulocationid': 'float64',  # using float64 to accommodate NaN values
    'dolocationid': 'float64',  # using float64 to accommodate NaN values
    'sr_flag': 'float64',       # using float64 to accommodate NaN values
    'affiliated_base_number': 'object',
    'dwid': 'object'
}


@data_exporter
def export_yellow_taxi_data_to_bigquery(df: DataFrame, **kwargs) -> None:
    project_id = 'terraform-demo-424912'
    dataset_id = 'nyc_taxi_data'
    table_id = 'fhv_cab_data'
    view_id = 'fhv_cab_data_view_staging'
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
            bigquery.SchemaField("dispatching_base_num", "STRING"),
            bigquery.SchemaField("fhpickup_datetime", "TIMESTAMP"),
            bigquery.SchemaField("fhdropoff_datetime", "TIMESTAMP"),
            bigquery.SchemaField("pulocationid", "FLOAT64"),
            bigquery.SchemaField("dolocationid", "FLOAT64"),
            bigquery.SchemaField("sr_flag", "FLOAT64"),
            bigquery.SchemaField("affiliated_base_number", "STRING"),
            bigquery.SchemaField("dwid", "STRING")]
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
        dispatching_base_num STRING,
        fhpickup_datetime TIMESTAMP,
        fhdropoff_datetime TIMESTAMP,
        pulocationid FLOAT64,
        dolocationid FLOAT64,
        sr_flag FLOAT64,
        affiliated_base_number STRING,
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
        dispatching_base_num = S.dispatching_base_num,
        fhpickup_datetime = S.fhpickup_datetime,
        fhdropoff_datetime = S.fhdropoff_datetime,
        pulocationid = S.pulocationid,
        dolocationid = S.dolocationid,
        sr_flag = S.sr_flag,
        affiliated_base_number = S.affiliated_base_number,
        dwid = S.dwid
    WHEN NOT MATCHED THEN
      INSERT (
        dispatching_base_num,
        fhpickup_datetime,
        fhdropoff_datetime,
        pulocationid,
        dolocationid,
        sr_flag,
        affiliated_base_number,
        dwid
      )
      VALUES (
        S.dispatching_base_num,
        S.fhpickup_datetime,
        S.fhdropoff_datetime,
        S.pulocationid,
        S.dolocationid,
        S.sr_flag,
        S.affiliated_base_number,
        S.dwid
      )
    """
    client.query(merge_query).result()