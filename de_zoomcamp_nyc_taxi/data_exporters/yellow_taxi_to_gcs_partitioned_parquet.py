import pyarrow as pa
import pyarrow.parquet as pq
import os
from google.cloud import storage
from mage_ai.data_preparation.decorators import data_exporter

# Define the bucket, project, and table  
bucket_name = 'terraform-demo-424912-mage-zoompcamp-bucket'
project_id = 'terraform-demo-424912'
table_name = 'yellow_taxi_trip_data'      


root_path = f'{bucket_name}/{table_name}'

# Initialize Google Cloud Storage client
storage_client = storage.Client()

@data_exporter
def export_data(data, *args, **kwargs):
    """
    Exports data to Google Cloud Storage as partitioned Parquet files.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Output (optional):
        Optionally return any object and it'll be logged and
        displayed when inspecting the block run.
    """
    # Create PyArrow Table from DataFrame
    table = pa.Table.from_pandas(data)

    # Get GCS Filesystem
    gcs = pa.fs.GcsFileSystem()

    # Define the GCS path
    date_str = data['tpep_pickup_date'].iloc[0].strftime('%Y-%m-%d')
    gcs_path = f'{root_path}/tpep_pickup_date={date_str}'

    # Check if the data already exists in GCS
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(gcs_path)
    if blob.exists():
        print(f"Data for {date_str} already exists in GCS. Skipping export.")
    else:
        # Export data to GCS as partitioned Parquet files
        pq.write_to_dataset(
            table, 
            root_path=root_path, 
            partition_cols=['tpep_pickup_date'], 
            filesystem=gcs
        )
        print(f"Data for {date_str} exported to GCS.")

    return data
