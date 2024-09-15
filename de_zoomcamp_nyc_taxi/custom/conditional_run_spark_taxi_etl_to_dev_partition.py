if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@custom
def transform_custom(*args, **kwargs):
    tripdata_type = kwargs['tripdata_type']

    if tripdata_type == 'yellow_cab_tripdata':
        return 'run_spark_yellow_taxi_etl_to_dev_partition'
    elif tripdata_type == 'green_cab_tripdata':
        return 'run_spark_green_taxi_etl_to_dev_partition'
    elif tripdata_type == 'fhv_cab_tripdata':
        return 'run_spark_fhv_taxi_etl_to_dev_partition'