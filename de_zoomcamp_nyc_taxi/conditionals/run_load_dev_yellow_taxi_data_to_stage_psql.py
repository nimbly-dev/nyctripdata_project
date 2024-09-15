if 'condition' not in globals():
    from mage_ai.data_preparation.decorators import condition


@condition
def evaluate_condition(data, *args, **kwargs) -> bool:
    if data == 'run_export_dev_pq_yellow_taxi_data_to_stage_psql':
        return True
    else:
        return False
