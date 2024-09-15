from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.google_cloud_storage import GoogleCloudStorage
from pandas import DataFrame
from os import path
import logging
import os

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = './keys/my-creds.json'
bucket_name = 'terraform-demo-424912-mage-zoompcamp-bucket'
project_id = 'terraform-demo-424912'
table_name = 'yellow_cab_tripdata'

root_path = f'{table_name}'

@data_exporter
def export_data_to_google_cloud_storage(df: DataFrame, **kwargs) -> None:
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    # Partition the data by day
    df['partition_date'] = df['pickup_datetime'].dt.strftime('%Y-%m-%d')
    
    for date, group in df.groupby('partition_date'):
        object_key = f'{root_path}/partitioned_date={date}/data.parquet'
        
        GoogleCloudStorage.with_config(ConfigFileLoader(config_path, config_profile)).export(
            group,
            bucket_name,
            object_key
        )