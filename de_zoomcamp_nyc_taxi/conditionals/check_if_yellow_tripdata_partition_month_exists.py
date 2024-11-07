if 'condition' not in globals():
    from mage_ai.data_preparation.decorators import condition

import os

from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader, ConfigKey
from mage_ai.io.postgres import Postgres

@condition
def evaluate_condition(*args, **kwargs) -> bool:
    year_month = kwargs['year_month']
    config_profile = 'production_db'  
    config_path = os.path.join(get_repo_path(), 'io_config.yaml')
    source_table_name = f'yellow_cab_tripdata_production_{year_month}'

    config = ConfigFileLoader(config_path, config_profile)

    # with Postgres.with_config(config) as loader:
    #     query = f"""
    #     SELECT EXISTS (
    #         SELECT 1
    #         FROM information_schema.tables
    #         WHERE table_schema = 'public'
    #         AND table_name = '{source_table_name}'
    #     );
    #     """

    #     result = loader.execute(query)
    #     table_exists = result[0][0]  

    return False
