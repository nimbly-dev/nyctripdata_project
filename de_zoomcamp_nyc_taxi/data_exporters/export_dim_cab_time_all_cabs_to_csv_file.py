from mage_ai.io.postgres import Postgres
from mage_ai.io.config import ConfigFileLoader, ConfigKey
from mage_ai.settings.repo import get_repo_path
from de_zoomcamp_nyc_taxi.utils.sql.sql_util import get_service_account
import pandas as pd
import os
from mage_ai.io.file import FileIO

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data_to_file(df: pd.DataFrame, **kwargs) -> None:
    # Define configuration path and profile
    config_profile = 'production_db'
    config_path = os.path.join(get_repo_path(), 'io_config.yaml')
    
    # Load database configuration
    config = ConfigFileLoader(config_path, config_profile)
    db_name = config[ConfigKey.POSTGRES_DBNAME]
    host_name = config[ConfigKey.POSTGRES_HOST]
    host_port = config[ConfigKey.POSTGRES_PORT]
    service_account = get_service_account('production')
    service_account_name = service_account['service_account_name']
    service_account_password = service_account['service_account_password']


    with Postgres.with_config(ConfigFileLoader(config_path, config_profile)) as loader:

        query_to_get_time_all_cabs = "SELECT * FROM dim.dim_cab_time_all_cabs_mv;"
        

        get_time_all_cabs_df = loader.load(query_to_get_time_all_cabs)
    
    filepath = '/home/src/de_zoomcamp_nyc_taxi/resources/csv/dim_cab_time_all_cabs.csv'
    get_time_all_cabs_df.to_csv(filepath, index=False)
