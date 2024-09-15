if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@custom
def transform_custom(*args, **kwargs):
    tripdata_type = kwargs['tripdata_type']

    if tripdata_type == 'yellow_cab_tripdata':
        return 'run_clean_yellow_cab_tripdata'
    elif tripdata_type == 'green_cab_tripdata':
        return 'run_clean_green_cab_tripdata'
    elif tripdata_type == "fhv_cab_tripdata":
        return 'run_clean_fhv_cab_tripdata'

