from mage_ai.orchestration.triggers.api import trigger_pipeline
if 'callback' not in globals():
    from mage_ai.data_preparation.decorators import callback


@callback('success')
def trigger(parent_block_data, **kwargs):

    trigger_pipeline(
        'trigger_spark_load_to_psql_stage',       
        variables={},          
        check_status=True,  
        error_on_failure=True, 
        poll_interval=60,      
        poll_timeout=None,   
        verbose=True,        
    )
