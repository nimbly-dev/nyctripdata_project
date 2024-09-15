from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from os import path
from pandas import DataFrame

from sqlalchemy import func
from de_zoomcamp_nyc_taxi.utils.sql.sql_util import execute_function_on_postgres, run_sql_on_postgres

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform_in_postgres(*args, **kwargs) -> DataFrame:
    year = kwargs.get('year')
    month = kwargs.get('month')
    tripdata_type = kwargs['tripdata_type']
    environment= kwargs['configuration'].get('environment')

    if year is None or month is None or tripdata_type is None:
        raise ValueError("The 'year', 'month', and 'tripdata_type' parameters are required.")
    
    # Format target date and view name
    target_date = f"{year}-{month:02d}-01"
    view_name = f"{tripdata_type}_{year}{month:02d}_tempview_{environment}"
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    # Define the base SQL query for creating or replacing the view
    # create_view_query = f"""
    # CREATE OR REPLACE VIEW {view_name} AS
    # SELECT *
    # FROM {tripdata_type}_staging
    # WHERE tpep_pickup_datetime >= '{target_date}'
    # AND tpep_pickup_datetime < '{target_date}'::date + interval '1 month';
    # """

    # Define the SQL query for cleaning and selecting data
    clean_viewtemp_data = f"""
    SELECT DISTINCT ON (dwid) *
    FROM {view_name}
    WHERE pickup_datetime IS NOT NULL
    AND dropoff_datetime IS NOT NULL
    ORDER BY dwid, pickup_datetime;
    """

    with Postgres.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
        # # Create or replace the base view
        # loader.execute(create_view_query)
        # loader.commit()  # Apply the base view changes

        # Perform the transformation on the newly created view
        loader.execute(clean_viewtemp_data)
        loader.commit()  # Apply the transformation changes


        # Then, execute the function
        execute_function_query = f"SELECT create_partition_if_not_exists('yellow_cab_tripdata_production', '{target_date}'::date)"
        loader.execute(execute_function_query)
        loader.commit()
        
        # Load and return the sample from the transformed view
        return loader.load(f'SELECT * FROM {view_name}')
        
@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'