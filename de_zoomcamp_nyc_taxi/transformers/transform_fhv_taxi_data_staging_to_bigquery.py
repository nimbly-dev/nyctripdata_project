import pandas as pd
from mage_ai.shared.logger import logging

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

# Set the data types for each column
dtype_map = {
    'dispatching_base_num': 'object',
    'fhpickup_datetime': 'datetime64[ns]',
    'fhdropoff_datetime': 'datetime64[ns]',
    'pulocationid': 'float64',  # using float64 to accommodate NaN values
    'dolocationid': 'float64',  # using float64 to accommodate NaN values
    'sr_flag': 'float64',       # using float64 to accommodate NaN values
    'affiliated_base_number': 'object',
    'dwid': 'object'
}

logger = logging.getLogger(__name__)

@transformer
def transform(data, *args, **kwargs):
    logger.setLevel(kwargs.get('log_level'))

    # Convert columns to the specified types
    for column, dtype in dtype_map.items():
        if column in data.columns:
            data[column] = data[column].astype(dtype)
        else:
            # Add missing columns with appropriate type
            data[column] = pd.Series(dtype=dtype)

    # Rename columns to lowercase and replace spaces with underscores
    data.columns = data.columns.str.replace(' ', '_').str.lower()

    for column in data.columns:
        logger.debug(f'Column: {column}, Type: {data[column].dtype}')

    return data


@test
def test_output(output, *args) -> None:
    assert output is not None, 'The output is undefined'
    
    # Ensure all columns are present and have the correct data types
    for column, dtype in dtype_map.items():
        assert column in output.columns, f'Missing column: {column}'
        assert output[column].dtype == dtype, f'Column {column} has incorrect type: expected {dtype}, found {output[column].dtype}'