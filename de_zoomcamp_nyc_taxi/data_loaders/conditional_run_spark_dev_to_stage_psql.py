import io
import pandas as pd
import requests
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_api(*args, **kwargs):
    tripdata_type = kwargs['configuration'].get('tripdata_type')

    if tripdata_type == 'yellow_cab_tripdata':
        return 'run_yellow_cab_tripdata'
    else:
        return 'run_green_cab_tripdata'

