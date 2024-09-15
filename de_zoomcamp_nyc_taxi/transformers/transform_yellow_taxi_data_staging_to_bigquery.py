import pandas as pd
from mage_ai.shared.logger import logging

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

# Adjusted dtype_map to align with PostgreSQL schema
dtype_map = {
    'dwid': 'object',  # String Type, Enforced Unique Constraint for row data to be unique
    'vendor_id': 'Int64',
    'pickup_datetime': 'datetime64[ns]',
    'dropoff_datetime': 'datetime64[ns]',
    'passenger_count': 'Int64',
    'trip_distance': 'float64',
    'ratecode_id': 'Int64',
    'store_and_fwd_flag': 'object',
    'pu_location_id': 'Int64',
    'do_location_id': 'Int64',
    'payment_type': 'Int64',
    'fare_amount': 'float64',
    'extra': 'float64',
    'mta_tax': 'float64',
    'tip_amount': 'float64',
    'tolls_amount': 'float64',
    'improvement_surcharge': 'float64',
    'total_amount': 'float64',
    'congestion_surcharge': 'float64',
    'airport_fee': 'float64'
}

logger = logging.getLogger(__name__)

@transformer
def transform_yellow_taxi_data_staging_to_psql(data, *args, **kwargs):
    logger.setLevel(kwargs.get('log_level'))

    # Convert columns to the specified types
    for column, dtype in dtype_map.items():
        if column in data.columns:
            data[column] = data[column].astype(dtype)
        else:
            # Add missing columns with appropriate type
            data[column] = pd.Series(dtype=dtype)

    # Rename columns to match PostgreSQL schema
    data.columns = data.columns.str.replace(' ', '_').str.lower()

    for column in data.columns:
        logger.debug(f'Column: {column}, Type: {data[column].dtype}')

    # Ensure primary key columns are not null
    primary_key_columns = ['dwid', 'pickup_datetime', 'dropoff_datetime']
    for col in primary_key_columns:
        if col in data.columns:
            data[col] = data[col].dropna()
        else:
            # Add primary key columns if not present
            data[col] = pd.Series(dtype='object')

    return data

@test
def test_output(output, *args) -> None:
    assert output is not None, 'The output is undefined'
    
    # Ensure all columns are present and have the correct data types
    for column, dtype in dtype_map.items():
        assert column in output.columns, f'Missing column: {column}'
        assert output[column].dtype == dtype, f'Column {column} has incorrect type: expected {dtype}, found {output[column].dtype}'

    # Check if primary key columns are not null
    primary_key_columns = ['dwid', 'pickup_datetime', 'dropoff_datetime']
    for col in primary_key_columns:
        assert col in output.columns, f'Missing primary key column: {col}'
        assert not output[col].isnull().any(), f'Primary key column {col} contains null values'
