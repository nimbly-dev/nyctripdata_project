import os
import pandas as pd
from mage_ai.shared.logger import logging
from mage_ai.services.spark.config import SparkConfig
from mage_ai.services.spark.spark import get_spark_session
from mage_ai.data_preparation.repo_manager import RepoConfig, get_repo_path

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

logger = logging.getLogger(__name__)

def find_parquet_files(directory):
    """
    Recursively find all Parquet files in a directory.
    """
    parquet_files = []
    for root, _, files in os.walk(directory):
        for file in files:
            if file.endswith('.parquet'):
                parquet_files.append(os.path.join(root, file))
    return parquet_files


@data_loader
def load_data_from_api(*args, **kwargs):
    """
    Load partitioned data from Parquet files and return as Pandas DataFrame.
    """
    year = kwargs['year']
    month = kwargs['month']
    shared_dir = os.getenv('SHARED_DATA_DIR', '/opt/spark/spark-warehouse')
    partitioned_path = os.path.join(shared_dir, f'partitioned/{year}/{month:02}/')

    # Initialize Spark session
    repo_config = RepoConfig(repo_path=get_repo_path())
    spark_config = SparkConfig.load(config=repo_config.spark_config)
    spark = get_spark_session(spark_config)

    # Read the partitioned Parquet files into a Spark DataFrame
    try:
        df = spark.read.parquet(partitioned_path)

        # Find all parquet files in the partitioned path
        partitioned_files = find_parquet_files(partitioned_path)
        if not partitioned_files:
            logger.error(f"No parquet files found in partitioned path: {partitioned_path}")
            return None
        logger.info(f"Loaded data from {partitioned_path} into Spark DataFrame with {df.count()} records")
        
        # Convert to Pandas DataFrame
        # pandas_df = pd.concat([pd.read_parquet(f) for f in partitioned_files], ignore_index=True)
        logger.info(df)
        logger.info(f"DataFrame counts: {df.count()}")
        return df
    except Exception as e:
        logger.error(f"Error loading data from parquet files: {str(e)}")
        return None


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
