from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from pandas import DataFrame
from os import path

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

#Reason to noy use Upsert is to reduce performance overhead
#A scheduled pipeline will handle the data on Postgres
@data_exporter
def export_data_to_postgres(df: DataFrame, **kwargs) -> None:
    schema_name = kwargs['configuration'].get('schema_name')          
    table_name = kwargs['configuration'].get('config_profile')    
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = kwargs['configuration'].get('table_name')

    with Postgres.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
        
        loader.export(
            df,
            schema_name,
            table_name,
            index=False,  
            if_exists='append'  
        )