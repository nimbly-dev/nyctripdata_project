if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@custom
def transform_custom(*args, **kwargs):
    env_to_populate =kwargs['env_to_populate']

    if env_to_populate == 'stage':
        return 'run_trigger_fact_trip_data_to_stage'
    elif env_to_populate == 'production':
        return 'run_trigger_fact_trip_data_to_prod'
    else:
        return "Please provide env_to_populate parameter"