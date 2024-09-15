from mage_ai.data_preparation.repo_manager import RepoConfig, get_repo_path
from mage_ai.services.spark.config import SparkConfig
from mage_ai.services.spark.spark import get_spark_session

from os import path
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_big_query(*args, **kwargs):
    repo_config = RepoConfig(repo_path=get_repo_path())
    spark_config = SparkConfig.load(
        config=repo_config.spark_config)

    spark = get_spark_session(spark_config)

    # Use gs:// URI for GCS path
    gcs_path = 'gs://terraform-demo-424912-mage-zoompcamp-bucket/yellow_taxi_trip_data/partitioned_date=*/*'
    df_green = spark.read.parquet(gcs_path)

    print(f'Number of partitions: {df_green.rdd.getNumPartitions()}')
    
    return df_green


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
