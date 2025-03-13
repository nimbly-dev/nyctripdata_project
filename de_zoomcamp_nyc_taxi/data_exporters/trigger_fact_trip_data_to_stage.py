import subprocess
import ast
if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

from mage_ai.orchestration.triggers.api import trigger_pipeline

def truncate_staging_tables(year_month):
    """
    Truncates monthly partitions for the four staging tables 
    (stg_yellow_cab_tripdata, stg_green_cab_tripdata, stg_fhv_cab_tripdata, combine_clean_cab_tripdata)
    in the 'staging' schema for the given year_month.
    """
    tables_to_clean = [
        'stg_yellow_cab_tripdata',
        'stg_green_cab_tripdata',
        'stg_fhv_cab_tripdata',
        'combine_clean_cab_tripdata',
    ]

    for table in tables_to_clean:
        cmd = [
            'dbt',
            'run-operation',
            'truncate_partition',  # or 'drop_old_partition' if you prefer dropping
            '--args', f'{{ "schema_name": "staging", "base_table_name": "{table}", "year_month": "{year_month}" }}'
        ]
        print(f'🧹 Cleaning table partition: {table} for {year_month} ...')
        subprocess.run(cmd, check=True)


@data_exporter
def trigger(*args, **kwargs):
    start_year = kwargs['start_year']
    start_month = kwargs['start_month']
    end_year = kwargs['end_year']
    end_month = kwargs['end_month']
    get_data_from = kwargs.get('get_data_from', [])

    if isinstance(get_data_from, str):
        get_data_from = ast.literal_eval(get_data_from)

    for year in range(start_year, end_year + 1):
        for month in range(1, 13):
            if (year == start_year and month < start_month) or (year == end_year and month > end_month):
                continue

            year_month = f"{year}_{month:02d}"

            # 2) Run the pipeline for this month
            try:
                trigger_pipeline(
                    'fact_trip_data_to_stage',
                    variables={
                        "year_month": year_month,
                        "get_data_from": get_data_from
                    },
                    check_status=True,
                    error_on_failure=True,
                    poll_interval=60,
                    poll_timeout=None,
                    verbose=True,
                )
            except Exception as e:
                print(f"⚠️ Failed for {year_month}: {str(e)}")

            # 1) Clean the monthly partitions (truncate or drop)
            try:
                truncate_staging_tables(year_month)
            except Exception as e:
                print(f"⚠️ Failed to cleanup data for {year_month}: {str(e)}")
                # Optionally continue or break here

