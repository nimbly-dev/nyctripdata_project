if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    # Remove rows where passenger count is equal to 0 and trip distance is equal to zero
    data = data[(data['passenger_count'] > 0) \
                                    & (data['trip_distance'] > 0)]

    # Rename columns in Camel Case to Snake Case
    data = data.rename(columns=lambda x: x.lower())

    return data


@test
def test_output(output, *args) -> None:
    assert output['vendorid'].isin(output['vendorid']).all(), "Assertion failed: vendor_id is not one of the existing values in the column."
    assert (output['passenger_count'] > 0).all(), "Assertion failed: passenger_count is not greater than 0."
    assert (output['trip_distance'] > 0).all(), "Assertion failed: trip_distance is not greater than 0."

