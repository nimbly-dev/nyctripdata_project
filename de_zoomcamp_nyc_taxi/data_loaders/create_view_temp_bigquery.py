from mage_ai.settings.repo import get_repo_path
from mage_ai.io.bigquery import BigQuery
from mage_ai.io.config import ConfigFileLoader
from os import path
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_big_query(*args, **kwargs):
    year = kwargs['year']
    month = kwargs['month']
    spark_mode = kwargs.get('spark_mode')
    dataset_type = kwargs['dataset_type']
    bq_temp_table_id = f'{dataset_id}.temp_table_for_{year}_{month:02d}_view'
    # Create or replace the view
    create_view_query = f"""
    CREATE OR REPLACE VIEW `{view_id}` AS
    SELECT * FROM `{bq_temp_table_id}`
    """
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    BigQuery.with_config(ConfigFileLoader(config_path, config_profile)).load(create_view_query)


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'