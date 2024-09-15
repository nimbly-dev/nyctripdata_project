if 'condition' not in globals():
    from mage_ai.data_preparation.decorators import condition


@condition
def evaluate_condition(data,*args, **kwargs) -> bool:
    if data == 'run_clean_green_cab_tripdata':
        return True
    else:
        return False
