from mage_ai.orchestration.run_status_checker import check_status

if 'sensor' not in globals():
    from mage_ai.data_preparation.decorators import sensor


@sensor
def check_condition(*args, **kwargs) -> bool:
    """
    Checks if either 'spark_clean_yellow_taxi_data' or 'spark_clean_green_taxi_data' blocks have completed.
    """
    execution_date = kwargs['execution_date']
    hours = 1  

    yellow_taxi_status = check_status(
        'spark_load_to_psql_stage',
        execution_date,
        block_uuid='spark_clean_yellow_taxi_data',  
        hours=hours,
    )

    green_taxi_status = check_status(
        'spark_load_to_psql_stage',
        execution_date,
        block_uuid='spark_clean_green_taxi_data',  
        hours=hours,
    )

    fhv_taxi_status = check_status(
        'spark_load_to_psql_stage',
        execution_date,
        block_uuid='spark_clean_fhv_taxi_data',  
        hours=hours,
    )

    return yellow_taxi_status or green_taxi_status or fhv_taxi_status