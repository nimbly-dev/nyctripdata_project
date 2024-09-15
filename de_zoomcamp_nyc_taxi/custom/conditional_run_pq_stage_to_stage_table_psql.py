if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@custom
def transform_custom(*args, **kwargs):
    tripdata_type = kwargs['tripdata_type']

    if tripdata_type == 'yellow_cab_tripdata':
        return 'run_export_dev_pq_yellow_taxi_data_to_stage_psql'
    elif tripdata_type == 'green_cab_tripdata':
        return 'run_export_dev_pq_green_taxi_data_to_stage_psql'
    elif tripdata_type == 'fhv_cab_tripdata':
        return 'run_export_dev_pq_fhv_taxi_data_to_stage_psql'
