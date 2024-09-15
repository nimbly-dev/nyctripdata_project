if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

from de_zoomcamp_nyc_taxi.utils.sql.sql_util import run_sql_on_postgres


def check_view_exists(engine_name, view_name, schema='public'):
    """
    Check if a view exists in the specified schema of the PostgreSQL database.
    """
    schema = schema.lower()
    view_name = view_name.lower()

    query = f"""
    SELECT EXISTS (
        SELECT 1
        FROM information_schema.views
        WHERE table_schema = '{schema}'
        AND table_name = '{view_name}'
    );
    """

    result = run_sql_on_postgres(query, engine_name)
    return result;


@data_loader
def load_data(*args, **kwargs):
    year = kwargs.get('year')
    month = kwargs.get('month')
    tripdata_type = kwargs['tripdata_type']
    databaseurl = kwargs['configuration'].get('databaseurl')
    dbname = kwargs['configuration'].get('dbname')
    environment = kwargs['configuration'].get('environment')
    LOG = kwargs.get('logger')

    if year is None or month is None or tripdata_type is None:
        raise ValueError("The 'year', 'month', and 'tripdata_type' parameters are required.")
    
    # Format target date and view name
    target_date = f"{year}-{month:02d}-01"
    view_name = f"temp.{tripdata_type}_{year}{month:02d}_tempview_{environment}"
    engine_name = f'postgresql://postgres:postgres@{databaseurl}/{dbname}'
    table_name = f'{tripdata_type}_{environment}'

    # Create a view to extract year and month selected data
    create_view_query = f"""
    CREATE OR REPLACE VIEW {view_name} AS
    WITH cleaned_data AS (
        SELECT *
        FROM {table_name}
        WHERE pickup_datetime >= '{target_date}'
        AND pickup_datetime < '{target_date}'::date + interval '1 month'
    )
    SELECT * FROM cleaned_data;
    """
    result = run_sql_on_postgres(create_view_query, engine_name)

    view_exists = check_view_exists(engine_name, view_name, schema='temp')
    return view_exists


@test
def test_output(output, **kwargs) -> None:
    """
    Test to verify that the view has been created successfully.
    """
    year = kwargs.get('year')
    month = kwargs.get('month')
    tripdata_type = kwargs['tripdata_type']
    databaseurl = kwargs['configuration'].get('databaseurl')
    dbname = kwargs['configuration'].get('dbname')
    environment = kwargs['configuration'].get('environment')
    LOG = kwargs.get('logger')
    
    view_name = f"{tripdata_type}_{year}{month:02d}_tempview_{environment}"
    engine_name = f'postgresql://postgres:postgres@{databaseurl}/{dbname}'

    assert output is not None, 'The output is undefined'
    assert output, f"View '{view_name}' does not exist in the 'temp' schema."
    