from mage_ai.orchestration.run_status_checker import check_status

if 'sensor' not in globals():
    from mage_ai.data_preparation.decorators import sensor


@sensor
def check_condition(*args, **kwargs) -> bool:
    """
    Checks if either 'trigger_spark_yellow_taxi_etl_to_dev_partition' or 'trigger_spark_green_taxi_etl_to_dev_partition' or 'trigger_spark_fhv_taxi_etl_to_dev_partition' blocks have completed.
    """
    execution_date = kwargs['execution_date']
    hours = 1  

    yellow_taxi_status = check_status(
        'spark_populate_tripdata_local_infastructure',
        execution_date,
        block_uuid='trigger_spark_yellow_taxi_etl_to_dev_partition',  
        hours=hours,
    )

    green_taxi_status = check_status(
        'spark_populate_tripdata_local_infastructure',
        execution_date,
        block_uuid='trigger_spark_green_taxi_etl_to_dev_partition',  
        hours=hours,
    )

    fhv_taxi_status = check_status(
        'spark_populate_tripdata_local_infastructure',
        execution_date,
        block_uuid='trigger_spark_fhv_taxi_etl_to_dev_partition',  
        hours=hours,
    )

    return yellow_taxi_status or green_taxi_status or fhv_taxi_status