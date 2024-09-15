from pyspark.sql import DataFrame
import os

def validate_parquet_files(directory: str) -> bool:
    """
    Validate the presence of Parquet files in the given directory.
    :param directory: Path to the directory to check.
    :return: True if at least one Parquet file is found, otherwise False.
    """
    if not os.path.exists(directory):
        return False

    # Check if any file in the directory has a .parquet extension
    for file_name in os.listdir(directory):
        if file_name.endswith('.parquet'):
            return True
    
    return False
