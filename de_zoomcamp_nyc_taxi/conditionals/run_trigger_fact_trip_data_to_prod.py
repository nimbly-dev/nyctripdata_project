if 'condition' not in globals():
    from mage_ai.data_preparation.decorators import condition


@condition
def evaluate_condition(data, *args, **kwargs) -> bool:
    if data == 'run_trigger_fact_trip_data_to_stage':
        return True
    else:
        return False
