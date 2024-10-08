from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from pandas import DataFrame
from os import path

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data_to_postgres(df: DataFrame, **kwargs) -> None:
    schema_name = 'public' 
    table_name = kwargs['configuration'].get('production_table_name')  
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = kwargs['configuration'].get('config_profile')
    year = kwargs.get('year')
    month = kwargs.get('month')
    target_date = f"{year}-{month:02d}-01"

    type(df)

    with Postgres.with_config(ConfigFileLoader(config_path, config_profile)) as loader:

        # Then, execute the function
        execute_function_query = f"SELECT create_partition_if_not_exists('{table_name}', '{target_date}'::date)"
        loader.execute(execute_function_query)
        loader.commit()

        loader.export(
            df,
            schema_name,
            table_name,
            if_exists='replace',  
            allow_reserved_words=True,
        )