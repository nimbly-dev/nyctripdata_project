import dlt
import requests
from io import BytesIO
import pandas as pd
from mage_ai.shared.logger import logging

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

logger = logging.getLogger(__name__)

def download_data(year, month, base_url, headers):
    """
    Download data for a specific year and month.
    """
    url = f"{base_url}{year}-{month:02}.parquet"
    logger.info(f"Processing URL: {url}")

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()

        file_stream = BytesIO(response.content)
        df = pd.read_parquet(file_stream)
        return df

    except requests.exceptions.HTTPError as err:
        logger.error(f"Failed to retrieve data for {year}-{month:02}, HTTP error: {err}")
    except Exception as err:
        logger.error(f"Failed to retrieve data for {year}-{month:02}, Error: {err}")
        return None

@data_loader
def load_data(*args, **kwargs):
    """
    Load data from multiple files using dlt.
    """
    start_year = 2022
    start_month = 1
    end_year = 2023
    end_month = 6
    base_url = 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_'
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }

    data = []
    for year in range(start_year, end_year + 1):
        end_month_current_year = 12 if year < end_year else end_month
        for month in range(1, end_month_current_year + 1):
            if (year == start_year and month < start_month) or (year == end_year and month > end_month):
                continue

            df = download_data(year, month, base_url, headers)
            if df is not None:
                data.append(df)

    # Use dlt to load and concatenate data
    pipeline = dlt.pipeline(
        pipeline_name='yellow_taxi_pipeline',
        destination='duckdb',  # Specify your destination here, e.g., 'bigquery', 'postgresql', etc.
        dataset_name='yellow_taxi_data'
    )
    # Convert list of DataFrames to list of dictionaries for dlt
    records = [df.to_dict(orient='records') for df in data]
    flattened_records = [item for sublist in records for item in sublist]
    load_info = pipeline.run(flattened_records, table_name='yellow_tripdata')
    return load_info

@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'


