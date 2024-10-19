from mage_ai.orchestration.triggers.api import trigger_pipeline
if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def trigger(*args, **kwargs):
    start_year = kwargs['start_year']
    start_month = kwargs['start_month']
    end_year = kwargs['end_year']
    end_month = kwargs['end_month']

    # Loop through the years and months
    for year in range(start_year, end_year + 1):
        for month in range(1, 13):
            # Ensure that the month falls within the range for the start and end years
            if (year == start_year and month < start_month) or (year == end_year and month > end_month):
                continue
            
            # Construct the year_month in the format "YYYY_MM"
            year_month = f"{year}_{month:02d}"
            
            # Trigger the pipeline for each year_month
            trigger_pipeline(
                'fact_tripdata_to_production',
                variables={
                    "year_month": year_month
                },
                check_status=True,
                error_on_failure=True,
                poll_interval=60,
                poll_timeout=None,
                verbose=True,
            )