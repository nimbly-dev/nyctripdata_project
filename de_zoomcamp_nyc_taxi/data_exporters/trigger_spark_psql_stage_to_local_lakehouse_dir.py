from mage_ai.orchestration.triggers.api import trigger_pipeline
if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def trigger(*args, **kwargs):
    trigger_pipeline(
        'spark_psql_stage_to_local_lakehouse_dir', 
        variables={
            'tripdata_type': kwargs['tripdata_type'],
            'spark_mode': kwargs['spark_mode'],
            'start_month': kwargs['start_month'],
            'start_year': kwargs['start_year'],
            'end_month': kwargs['end_month'],
            'end_year': kwargs['end_year'],
            'pipeline_run_name': kwargs['pipeline_run_name']
        },               
        check_status=True,     
        error_on_failure=True,
        poll_interval=60,      
        poll_timeout=None,     
        verbose=True,   
    )