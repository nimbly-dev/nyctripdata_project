from mage_ai.orchestration.triggers.api import trigger_pipeline
if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def trigger(*args, **kwargs):
    dim_pipelines = kwargs['dim_pipelines']

    for pipeline_name in dim_pipelines:
        pipeline = f'dim_{pipeline_name}_pipeline'

        try:
            trigger_pipeline(
                pipeline,        
                variables={},           
                check_status=True,     
                error_on_failure=True,
                poll_interval=60,      
                poll_timeout=None,     
                verbose=True,   
            )
        except Exception as e:
            if 'does not exist' in str(e):
                print(f"Caught specific error: {e}")
            else:
                raise e
