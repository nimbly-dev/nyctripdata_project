from mage_ai.orchestration.triggers.api import trigger_pipeline
if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter
from concurrent.futures import ThreadPoolExecutor, as_completed

@data_exporter
def trigger(*args, **kwargs):
    start_year      = kwargs['start_year']
    start_month     = kwargs['start_month']
    end_year        = kwargs['end_year']
    end_month       = kwargs['end_month']
    parrelel_exc_count = kwargs.get('parrelel_exc_count', 5)

    tasks = []
    with ThreadPoolExecutor(max_workers=parrelel_exc_count) as executor:
        for year in range(start_year, end_year + 1):
            for month in range(1, 13):
                # Skip months outside the valid range.
                if (year == start_year and month < start_month) or (year == end_year and month > end_month):
                    continue

                year_month = f"{year}_{month:02d}"
                pipeline_run_name = f"{kwargs['pipeline_run_name']}_{year_month}"

                tasks.append(executor.submit(
                    trigger_pipeline,
                    'spark_fhv_taxi_etl_to_dev_partition',
                    variables={
                        "dev_limit_rows": kwargs['dev_limit_rows'],
                        'tripdata_type': kwargs['tripdata_type'],
                        'spark_mode': kwargs['spark_mode'],
                        'pipeline_run_name': pipeline_run_name,
                        'year_month': year_month,
                    },
                    check_status=True,
                    error_on_failure=True,
                    poll_interval=60,
                    poll_timeout=None,
                    verbose=True,
                ))

        # Wait for all tasks to complete.
        for future in as_completed(tasks):
            future.result()
