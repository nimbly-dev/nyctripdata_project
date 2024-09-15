if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform_staged_data(data, *args, **kwargs):
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
        'airport_fee': 'float64', 
        'dwid' : 'object' #String Type, A enforced Unique Constraint for row data to be unique
    }
    

    data.columns = (data.columns
                    .str.replace(' ', '_')
                    .str.lower()
    )

    return data

@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
