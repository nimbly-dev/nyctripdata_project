from os import path
from pandas import DataFrame

from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from de_zoomcamp_nyc_taxi.utils.sql.sql_util import execute_function_on_postgres, run_sql_on_postgres

# Import decorators dynamically
if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

@transformer
def transform_in_postgres(*args, **kwargs) -> DataFrame:
    """
    Transforms data in PostgreSQL and returns the result as a DataFrame.
    """
    LOG = kwargs.get('logger')

    # Extract parameters
    year = kwargs.get('year')
    month = kwargs.get('month')
    tripdata_type = kwargs['tripdata_type']
    source_environment = kwargs['configuration'].get('source_environment')
    schema = kwargs['configuration'].get('schema')

    # Validate parameters
    if year is None or month is None or tripdata_type is None:
        LOG.error("The 'year', 'month', and 'tripdata_type' parameters are required.")
        raise ValueError("The 'year', 'month', and 'tripdata_type' parameters are required.")
    
    # Format target date and view name
    target_date = f"{year}-{month:02d}-01"
    view_name = f"temp.{tripdata_type}_{year}{month:02d}_tempview_{source_environment}"
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = kwargs['configuration'].get('config_profile')

    # Define SQL query
    clean_viewtemp_data = f"""
    SELECT DISTINCT ON (dwid) *
    FROM {view_name}
    WHERE pickup_datetime IS NOT NULL
    AND dropoff_datetime IS NOT NULL
    ORDER BY dwid, pickup_datetime;
    """

    LOG.info(f"Executing query: {clean_viewtemp_data}")

    try:
        with Postgres.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
            # Execute SQL query and load data
            loader.execute(clean_viewtemp_data)
            loader.commit()  # Apply the transformation changes
            LOG.info(f"Transformation applied successfully to view: {view_name}")

            result_df = loader.load(f'SELECT * FROM {view_name}')
            LOG.info(f"Loaded data from view: {view_name} successfully.")
            # Print columns with their data types
            print(result_df.dtypes)
            return result_df
    
    except Exception as e:
        LOG.error(f"An error occurred: {e}")
        raise

@test
def test_output(output, *args, **kwargs) -> None:
    """
    Tests the output of the transformation.
    """
    LOG = kwargs.get('logger')
    LOG.info("Testing output...")
    assert output is not None, 'The output is undefined'
    LOG.info("Output test passed.")
