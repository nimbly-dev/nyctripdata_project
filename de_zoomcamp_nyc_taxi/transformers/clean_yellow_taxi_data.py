import pandas as pd
import logging
import os

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

logger = logging.getLogger(__name__)

@transformer
def transform(data, *args, **kwargs):
    """
    Clean the DataFrame according to specified rules.
    """
    year = kwargs.get('year')
    month = kwargs.get('month')
    LOG = kwargs.get('logger', logger)

    if not year or not month:
        raise ValueError("Error: 'year' and 'month' must be provided.")

    # Remove rows where trip_distance or passenger_count is <= 0
    data = data[(data['trip_distance'] > 0) & (data['passenger_count'] > 0)]

    # Remove rows where trip_distance or fare_amount is not within valid ranges
    data = data[(data['trip_distance'] > 0) & (data['trip_distance'] <= 500)]
    data = data[(data['fare_amount'] > 0) & (data['fare_amount'] <= 1000)]

    # Remove rows where pickup_datetime equals dropoff_datetime
    data = data[data['pickup_datetime'] != data['dropoff_datetime']]

    # Handle the store_and_fwd_flag column to be just 'Y' or 'N'
    data['store_and_fwd_flag'] = data['store_and_fwd_flag'].str[:1]

    # Add year and month columns for partitioning
    data['year'] = data['pickup_datetime'].dt.year
    data['month'] = data['pickup_datetime'].dt.month


    return data

@test
def test_all_conditions(*args, **kwargs) -> None:
    """
    Test multiple conditions on the cleaned data.
    """
    year = kwargs.get('year')
    month = kwargs.get('month')


    assert output is not None, 'The output DataFrame is undefined'

    # Check for duplicates
    assert output.shape[0] == output.drop_duplicates().shape[0], "The output DataFrame contains duplicate rows"

    # Check trip_distance and fare_amount ranges
    assert output[(output['trip_distance'] <= 0) | (output['trip_distance'] > 500)].empty, "Unreasonable trip distances"
    assert output[(output['fare_amount'] <= 0) | (output['fare_amount'] > 1000)].empty, "Unreasonable fare amounts"

    # Check for zero trip_distance or passenger_count
    assert output[(output['trip_distance'] == 0) | (output['passenger_count'] == 0)].empty, "Zero trip_distance or passenger_count"

    # Check for same pickup and dropoff times
    assert output[output['pickup_datetime'] == output['dropoff_datetime']].empty, "Same pickup and dropoff times"

    # Check store_and_fwd_flag values
    assert output['store_and_fwd_flag'].isin(['Y', 'N']).all(), "Invalid store_and_fwd_flag"
