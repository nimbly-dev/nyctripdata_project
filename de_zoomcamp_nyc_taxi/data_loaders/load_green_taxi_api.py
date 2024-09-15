import os
import pandas as pd
import requests
from mage_ai.shared.logger import logging

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

logger = logging.getLogger(__name__)

@data_loader
def load_data(*args, **kwargs):
    """
    Load data from multiple files and yield them one by one.
    """
    start_year = 2018
    start_month = 1
    end_year = 2024
    end_month = 12
    # Define the base URL for the data files
    base_url = 'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_'
    local_data_dir = 'data/green_taxi/'  # Directory to store downloaded files

    # Define headers
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }

    # Create local data directory if it doesn't exist
    os.makedirs(local_data_dir, exist_ok=True)

    # Inside the loop where URLs are generated
    for year in range(start_year, end_year + 1):
        # Determine the end month for the current year
        end_month_current_year = 12 if year < end_year else end_month
        for month in range(1, end_month_current_year + 1):
            # Skip months before the start date and after the end date
            if (year == start_year and month < start_month) or (year == end_year and month > end_month):
                continue
            
            # Generate the URL for the current year and month
            url = f"{base_url}{year}-{month:02}.parquet"
            local_file_path = f"{local_data_dir}green_tripdata_{year}-{month:02}.parquet"

            if os.path.exists(local_file_path):
                logger.info(f"File {local_file_path} already exists locally. Skipping download.")
                df = pd.read_parquet(local_file_path)
                yield df
                continue

            logger.info(f"Processing URL: {url}")

            try:
                # Make the HTTP request
                response = requests.get(url, headers=headers)
                response.raise_for_status()

                # Write the content to a local file
                with open(local_file_path, 'wb') as file:
                    file.write(response.content)

                # Read the Parquet file
                df = pd.read_parquet(local_file_path)
                yield df

            except requests.exceptions.HTTPError as err:
                logger.error(f"Failed to retrieve data for {year}-{month:02}, HTTP error: {err}")
            except Exception as err:
                logger.error(f"Failed to retrieve data for {year}-{month:02}, Error: {err}")

@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'

# Function to concatenate DataFrames
def concatenate_data():
    dfs = []
    for df in load_data():
        dfs.append(df)
    green_taxi_data = pd.concat(dfs, ignore_index=True)
    return green_taxi_data

# Calling the function to concatenate data
green_taxi_data = concatenate_data()
