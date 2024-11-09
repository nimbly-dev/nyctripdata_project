from mage_ai.orchestration.triggers.api import trigger_pipeline
if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def trigger(*args, **kwargs):
    start_year = kwargs['start_year']
    start_month = kwargs['start_month']
    end_year = kwargs['end_year']
    end_month = kwargs['end_month']

    for year in range(start_year, end_year + 1):
        for month in range(1, 13):

            if (year == start_year and month < start_month) or (year == end_year and month > end_month):
                continue
            
            year_month = f"{year}_{month:02d}"
            pipeline_run_name =  f"{kwargs['pipeline_run_name']}_{year_month}"

            trigger_pipeline(
                'spark_load_to_psql_stage', 
                variables={
                    'tripdata_type': kwargs['tripdata_type'],
                    'spark_mode': kwargs['spark_mode'],
                    'year_month': year_month,
                    'pipeline_run_name': pipeline_run_name,
                    'overwrite_enabled': kwargs['overwrite_enabled']
                },               
                check_status=True,     
                error_on_failure=True,
                poll_interval=60,      
                poll_timeout=None,     
                verbose=True,   
            )

