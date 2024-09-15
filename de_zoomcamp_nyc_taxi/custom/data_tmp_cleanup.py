if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test
import os
import glob
import shutil

SPARK_LAKEHOUSE_FILES_DIR = os.getenv('SPARK_LAKEHOUSE_DIR_FILES', '/opt/spark/spark-lakehouse/partitioned')
SPARK_LAKEHOUSE = os.getenv('SPARK_LAKEHOUSE_DIR', '/opt/spark/spark-lakehouse')

@custom
def cleanup(*args, **kwargs):
    tripdata_type = kwargs['tripdata_type']
    pipeline_run_name = kwargs['pipeline_run_name']

    base_lakehouse_pq_dir = os.path.join(SPARK_LAKEHOUSE_FILES_DIR, f'{tripdata_type}/tmp/pq/*/{pipeline_run_name}')
    downloads_dir = os.path.join(SPARK_LAKEHOUSE, '{tripdata_type}/')
    # Find all directories and files matching the pattern
    directories = glob.glob(base_lakehouse_pq_dir)

    # Loop through all found directories and delete them
    for directory in directories:
        try:
            # Remove entire directory tree
            shutil.rmtree(directory)
            print(f"Deleted directory: {directory}")
        except OSError as e:
            print(f"Error: {directory} : {e.strerror}")

    # Clean up the contents of downloads_dir without deleting the folder itself
    if os.path.exists(downloads_dir):
        for filename in os.listdir(downloads_dir):
            file_path = os.path.join(downloads_dir, filename)
            try:
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.remove(file_path)  # Remove the file
                    print(f"Deleted file: {file_path}")
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)  # Remove the directory
                    print(f"Deleted directory: {file_path}")
            except Exception as e:
                print(f"Failed to delete {file_path}. Reason: {e}")