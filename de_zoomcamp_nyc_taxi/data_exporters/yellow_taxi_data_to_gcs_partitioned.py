from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.google_cloud_storage import GoogleCloudStorage
from pandas import DataFrame
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.fs as fs
import io
import os
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = './keys/my-creds.json'
bucket_name = 'terraform-demo-424912-mage-zoompcamp-bucket'
project_id = 'terraform-demo-424912'
table_name = 'yellow_taxi_trip_data'     

root_path = f'{bucket_name}/{table_name}'

@data_exporter
def export_data_to_google_cloud_storage(data: DataFrame, **kwargs) -> None:
    data['partitioned_date'] = pd.to_datetime(data['tpep_pickup_datetime']).dt.date

    # define the pyarrow table and read the df into it
    table = pa.Table.from_pandas(data)

    gcs = pa.fs.GcsFileSystem()

    # write to the dataset using a parquet function
    pq.write_to_dataset(
        table, 
        root_path=root_path, 
        partition_cols=['partitioned_date'], # To be dropped on staging
        filesystem=gcs
    )