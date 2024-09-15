from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.google_cloud_storage import GoogleCloudStorage
from mage_ai.shared.logger import logging
from os import path
import pandas as pd
from datetime import datetime
import re
import pyarrow.parquet as pq

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

logger = logging.getLogger(__name__)

# Add variables
# dataset_name: block name
@data_loader
def load_from_google_cloud_storage(*args, **kwargs):
    logger.setLevel(kwargs.get('log_level'))
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    dataset_name = kwargs['configuration'].get('dataset_name')
    year = kwargs.get('year')
    month = kwargs.get('month')

    bucket_name = 'terraform-demo-424912-mage-zoompcamp-bucket'
    logger.info(f"Bucket Name: {bucket_name}")

    gcs = GoogleCloudStorage.with_config(ConfigFileLoader(config_path, config_profile))

    data_frames = []

    prefix = f'{dataset_name}/partitioned_date={year}-{month:02d}'
    logger.info(f"Prefix: {prefix}")
    
    # List all blobs in the directory with the specific prefix
    blobs = list(gcs.client.list_blobs(bucket_name, prefix=prefix))
    logger.debug(f"Blobs found: {[blob.name for blob in blobs]}")

    # Filter objects to match the required pattern
    pattern = re.compile(rf'{dataset_name}/partitioned_date=\d{{4}}-\d{{2}}-\d{{2}}/.*\.parquet$')
    filtered_blobs = [blob for blob in blobs if pattern.search(blob.name)]
    logger.debug(f"Filtered blobs: {[blob.name for blob in filtered_blobs]}")

    # Load all matching Parquet files into DataFrames
    for blob in filtered_blobs:
        try:
            logger.debug(f"Loading file: {blob.name}")
            with blob.open("rb") as f:
                table = pq.read_table(f)
                df = table.to_pandas()
                logger.debug(f"Loaded data shape: {df.shape}")
                if df.empty:
                    logger.warning(f"Warning: {blob.name} is empty.")
                else:
                    data_frames.append(df)
        except pd.errors.EmptyDataError:
            logger.error(f"Error: {blob.name} contains no data.")
        except Exception as e:
            logger.error(f"Error loading {blob.name}: {e}")

    if data_frames:
        combined_data = pd.concat(data_frames, ignore_index=True)
        logger.info(f'{year}-{month} taxi data num of rows exported {len(combined_data)}')
        return combined_data
    else:
        logger.info("No data found.")
        return pd.DataFrame()  # Return an empty DataFrame with the correct columns if no data is found

@test
def test_output(output, *args, **kwargs) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
    assert not output.empty, 'The output DataFrame is empty'

