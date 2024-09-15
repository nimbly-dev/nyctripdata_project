if 'condition' not in globals():
    from mage_ai.data_preparation.decorators import condition


@condition
def evaluate_condition(data, *args, **kwargs) -> bool:
    if data == 'run_spark_green_taxi_etl_to_dev_partition':
        return True
    else:
        return False
