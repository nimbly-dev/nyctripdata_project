if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
     # Drop rows with NaN in these columns
    data.dropna(subset=['pickup_datetime', 'dropOff_datetime', 'dispatching_base_num'], inplace=True) 

    # Remove duplicates
    data.drop_duplicates(inplace=True)

    # Rename columns in Camel Case to Snake Case
    data = data.rename(columns=lambda x: x.lower())

    return data


@test
def test_output(output, *args) -> None:
    assert output is not None, 'The output is undefined'
