from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from os import path
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_postgres(*args, **kwargs):
    year_month = kwargs['year_month']
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'stage_db'
    LOG = kwargs.get('logger')

    check_table_query = f"""
    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'fact' 
        AND table_name = 'fact_tripdata_{year_month}'
    )
    """

    data_query = f"""
    SELECT 
        dwid,
        cab_type,
        fare_amount,
        total_amount,
        trip_distance,
        ratecode_id,
        vendor_id,
        pu_location_id,
        do_location_id,
        pickup_datetime,
        dropoff_datetime,
        payment_type,
        dispatching_base_num,
        affiliated_base_number
    FROM 
        fact.fact_tripdata_{year_month}
    """

    empty_query = """
    SELECT 
        NULL::TEXT AS dwid,
        NULL::TEXT AS cab_type,
        NULL::REAL AS fare_amount,
        NULL::REAL AS total_amount,
        NULL::REAL AS trip_distance,
        NULL::INT AS ratecode_id,
        NULL::INT AS vendor_id,
        NULL::INT AS pu_location_id,
        NULL::INT AS do_location_id,
        NULL::TIMESTAMP AS pickup_datetime,
        NULL::TIMESTAMP AS dropoff_datetime,
        NULL::INT AS payment_type,
        NULL::VARCHAR AS dispatching_base_num,
        NULL::VARCHAR AS affiliated_base_number
    WHERE FALSE
    """

    with Postgres.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
        try:
            table_exists = loader.load(check_table_query)['exists'][0]
            if not table_exists:
                LOG.info(f"Table fact.fact_tripdata_{year_month} does not exist or 'yellow' is not in get_data_from.")
                return loader.load(empty_query)
            
            return loader.load(data_query)
        except Exception as e:
            LOG.error(f"Error occurred: {e}")
            return loader.load(empty_query)

