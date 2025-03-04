from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from pandas import DataFrame
from os import path

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

def create_partition(loader,year_month: str) -> None:
    sql = f"""
    DO $$
    DECLARE
        partition_date DATE := TO_DATE('{year_month}' || '_01', 'YYYY_MM_DD');
    BEGIN
        PERFORM public.create_partition_if_not_exists(
            'fact',               
            'fact_tripdata',        
            partition_date           
        );
    END $$;
    """
    loader.execute(sql)

@data_exporter
def export_data_to_postgres(df: DataFrame, **kwargs) -> None:
    year_month = kwargs['year_month']
    schema_name = 'fact'  
    table_name = f'fact_tripdata_{year_month}'  
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'stage_db'


    with Postgres.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
        create_partition(loader,year_month)

        loader.export(
            df,
            schema_name,
            table_name,
            index=False, 
            if_exists='replace',  
        )