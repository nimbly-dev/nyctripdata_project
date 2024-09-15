import pandas as pd

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


def preprocess_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Preprocess the dataframe to ensure correct data types and handle missing values.
    """
    # Column names based on the data dictionary
    expected_columns = [
        'VendorID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime', 'passenger_count', 
        'trip_distance', 'RatecodeID', 'store_and_fwd_flag', 'PULocationID', 'DOLocationID', 
        'payment_type', 'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount', 
        'improvement_surcharge', 'total_amount', 'congestion_surcharge', 'airport_fee'
    ]
    
    # Ensure the DataFrame has the correct columns
    if len(df.columns) != len(expected_columns):
        raise ValueError(f"Length mismatch: DataFrame has {len(df.columns)} columns, expected {len(expected_columns)} columns. Actual columns: {df.columns.tolist()}")

    df.columns = expected_columns

    # Set the data types for each column
    dtype_map = {
        'VendorID': 'Int64',
        'tpep_pickup_datetime': 'datetime64[ns]',
        'tpep_dropoff_datetime': 'datetime64[ns]',
        'passenger_count': 'Int64',
        'trip_distance': 'float64',
        'RatecodeID': 'Int64',
        'store_and_fwd_flag': 'object',
        'PULocationID': 'Int64',
        'DOLocationID': 'Int64',
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
    
    # Convert columns to appropriate data types
    for col, dtype in dtype_map.items():
        df[col] = df[col].astype(dtype)
    
    # Handle missing values (example: fill missing passenger_count with 1, others with 0 or mean)
    df['passenger_count'].fillna(0, inplace=True)
    df['trip_distance'].fillna(0, inplace=True)
    df.fillna(0, inplace=True)  # Fill remaining NaNs with 0

    return df

@transformer
def transform_yellow_taxi_data_column(data, *args, **kwargs):
    df = data.copy()
    df = preprocess_dataframe(df)
    return df

@test
def test_output(output, *args) -> None:
    assert output is not None, 'The output is undefined'
    
    # Check that columns exist
    expected_columns = [
        'VendorID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime', 'passenger_count', 
        'trip_distance', 'RatecodeID', 'store_and_fwd_flag', 'PULocationID', 'DOLocationID', 
        'payment_type', 'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount', 
        'improvement_surcharge', 'total_amount', 'congestion_surcharge', 'airport_fee'
    ]
    assert all(column in output.columns for column in expected_columns), 'Missing columns in the output'
    
    # Check data types
    dtype_map = {
        'VendorID': 'Int64',
        'tpep_pickup_datetime': 'datetime64[ns]',
        'tpep_dropoff_datetime': 'datetime64[ns]',
        'passenger_count': 'Int64',
        'trip_distance': 'float64',
        'RatecodeID': 'Int64',
        'store_and_fwd_flag': 'object',
        'PULocationID': 'Int64',
        'DOLocationID': 'Int64',
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
    for col, dtype in dtype_map.items():
        assert output[col].dtype == dtype, f'Column {col} has incorrect type {output[col].dtype}'
