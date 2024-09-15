if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


# Column names based on the data dictionary
expected_columns = [
    'dispatching_base_num','pickup_datetime','dropOff_datetime','PULocationID','DOLocationID',
    'SR_Flag','Affiliated_base_number'
]


@transformer
def transform(df, *args, **kwargs):
    if len(df.columns) != len(expected_columns):
        raise ValueError(f"Length mismatch: DataFrame has {len(df.columns)} columns, expected {len(expected_columns)} columns. Actual columns: {df.columns.tolist()}")

    df.columns = expected_columns

    # Set the data types for each column
    dtype_map = {
        'dispatching_base_num': 'object',
        'pickup_datetime': 'datetime64[ns]',
        'dropOff_datetime': 'datetime64[ns]',
        'PULocationID': 'float64',  # using float64 to accommodate NaN values
        'DOLocationID': 'float64',  # using float64 to accommodate NaN values
        'SR_Flag': 'float64',       # using float64 to accommodate NaN values
        'Affiliated_base_number': 'object'
    }

    # Convert columns to the specified data types
    for col, dtype in dtype_map.items():
        if col in df.columns:
            df[col] = df[col].astype(dtype)
        else:
            raise KeyError(f"Column {col} is missing from the DataFrame")

    return df


@test
def test_output(output, *args) -> None:
    assert output is not None, 'The output is undefined'
    
    assert all(column in output.columns for column in expected_columns), 'Missing columns in the output'
    
    # Check data types
    dtype_map = {
        'dispatching_base_num': 'object',
        'pickup_datetime': 'datetime64[ns]',
        'dropOff_datetime': 'datetime64[ns]',
        'PULocationID': 'float64',  # using float64 to accommodate NaN values
        'DOLocationID': 'float64',  # using float64 to accommodate NaN values
        'SR_Flag': 'float64',       # using float64 to accommodate NaN values
        'Affiliated_base_number': 'object'
    }
    for col, dtype in dtype_map.items():
        assert output[col].dtype == dtype, f'Column {col} has incorrect type {output[col].dtype}'
