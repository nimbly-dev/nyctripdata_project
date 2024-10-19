from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from pandas import DataFrame
from os import path

import pandas as pd

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

@data_exporter
def export_data_to_postgres(df: DataFrame, **kwargs) -> None:
    """
    Template for exporting data to a PostgreSQL database.
    Specify your configuration settings in 'io_config.yaml'.

    Docs: https://docs.mage.ai/design/data-loading#postgresql
    """
    schema_name = 'fact'  # Specify the name of the schema to export data to
    table_name = 'fact_tripdata'  # Specify the name of the table to export data to
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'stage_db'

    # Convert columns to the desired data types
    # df['dwid'] = df['dwid'].astype(str)  # Cast dwid to TEXT
    # df['cab_type'] = 'yellow'            # Set cab_type as a constant 'yellow'
    # df['fare_amount'] = df['fare_amount'].astype(float)  # Cast fare_amount to REAL
    # df['total_amount'] = df['total_amount'].astype(float)  # Cast total_amount to REAL
    # df['trip_distance'] = df['trip_distance'].astype(float)  # Cast trip_distance to REAL
    # df['ratecode_id'] = df['ratecode_id'].astype('Int32')  # Cast ratecode_id to INT (use 'Int32' for handling nulls)
    # df['vendor_id'] = df['vendor_id'].astype('Int32')  # Cast vendor_id to INT (use 'Int32' for handling nulls)
    # df['pu_location_id'] = df['pu_location_id'].astype('Int32')  # Cast pu_location_id to INT
    # df['do_location_id'] = df['do_location_id'].astype('Int32')  # Cast do_location_id to INT
    # df['pickup_datetime'] = pd.to_datetime(df['pickup_datetime'])  # Ensure pickup_datetime is datetime
    # df['dropoff_datetime'] = pd.to_datetime(df['dropoff_datetime'])  # Ensure dropoff_datetime is datetime
    # df['payment_type'] = df['payment_type'].astype('Int32')  # Cast payment_type to INT
    # df['dispatching_base_num'] = None  # Set dispatching_base_num as NULL
    # df['affiliated_base_number'] = None  # Set affiliated_base_number as NULL

    # Define schema and table for export
    # schema_name = 'fact'  
    # table_name = 'fact_tripdata'  
    # config_path = path.join(get_repo_path(), 'io_config.yaml')
    # config_profile = 'stage_db'

    # print(df.head())
    # Check for any rows that may still have NaNs (not applicable for integers anymore)
    print(df.isnull().sum())
    
    # with Postgres.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
    #     loader.export(
    #         df,
    #         schema_name,
    #         table_name,
    #         index=False,  # Specifies whether to include index in exported table
    #         if_exists='append',  # Specify resolution policy if table name already exists
    #     )