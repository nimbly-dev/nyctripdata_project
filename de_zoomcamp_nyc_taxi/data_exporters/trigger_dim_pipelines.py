from mage_ai.orchestration.triggers.api import trigger_pipeline
import os
import subprocess

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def trigger(*args, **kwargs):
    # Extract parameters
    dim_pipelines = kwargs.get('dim_pipelines', [])
    is_refresh_only = kwargs.get('is_refresh_only', False)


    user_code_path = os.getenv('USER_CODE_PATH')  
    project_path = f'{user_code_path}/dbt/dbt_cab_trip_data_analytics'
    dbt_profile = 'production'


    print('--- Re-initializing dbt seeds (full-refresh) ---')
    seed_cmd = [
        'dbt', 'seed',
        '--full-refresh',
        '--profiles-dir', project_path,
        '--project-dir', project_path,
        '--target', dbt_profile
    ]
    subprocess.run(seed_cmd, check=True)

    if is_refresh_only:
        print('--- is_refresh_only=True => Running dimension models only ---')
        run_cmd = [
            'dbt', 'run',
            '--full-refresh',            
            '--profiles-dir', project_path,
            '--project-dir', project_path,
            '--target', dbt_profile,
            '--select', 'path:models/production/dim'
        ]
        subprocess.run(run_cmd, check=True)

    else:
        print('--- is_refresh_only=False => Triggering Mage pipelines ---')
        for pipeline_name in dim_pipelines:
            pipeline = f'dim_{pipeline_name}_pipeline'
            print(f'--- Triggering pipeline: {pipeline} ---')
            try:
                trigger_pipeline(
                    pipeline,
                    variables={},
                    check_status=True,
                    error_on_failure=True,
                    poll_interval=60,    # check status every 60s
                    poll_timeout=None,   # no timeout
                    verbose=True,
                )
            except Exception as e:
                if 'does not exist' in str(e):
                    print(f'Caught specific error: {e}')
                else:
                    raise e