from pyspark.sql import SparkSession, DataFrame
from pyspark import SparkConf
from pyspark.sql.functions import col

import os 
import shutil

def get_spark_session(mode='local', additional_configs=None, appname='DataProcessingApp'):
    """
    Build and return a Spark session configured to connect to a Spark cluster or run locally.
    
    Parameters:
        mode (str): The mode in which to run Spark. Can be 'local' or 'cluster'. Default is 'local'.
        additional_configs (dict): Additional configurations to set for the Spark session.
        appname (str): The name of the Spark application. Default is 'DataProcessingApp'.
        
    Returns:
        SparkSession: Configured Spark session.
    """
    # Define the master URL based on the mode
    master_url = 'spark://spark-master:7077' if mode == 'cluster' else 'local[*]'

    # Start building the Spark session
    builder = SparkSession.builder \
        .appName(appname) \
        .master(master_url) \
        .config('spark.executor.cores', '2') \
        .config('spark.executor.memory', '3g') \
        .config('spark.driver.memory', '2g') \
        .config('spark.default.parallelism','6') #Adjust parrelism based on num. of workers: 2 * 3 =6 

    # Apply additional configurations if provided
    if additional_configs:
        for key, value in additional_configs.items():
            builder.config(key, value)

    # Create the Spark session
    spark = builder.getOrCreate()

    # Print applied configurations for verification
    print("Configured Spark Settings:")
    # List of keys to check, including both defaults and any additional ones
    keys_to_check = [
        'spark.executor.cores', 
        'spark.executor.memory', 
        'spark.driver.memory'
    ]
    
    # Include any additional configs provided by the user
    if additional_configs:
        keys_to_check.extend(additional_configs.keys())
    
    # Print the current configuration values
    for key in keys_to_check:
        value = spark.conf.get(key, 'Not Set')
        print(f"{key}: {value}")

    return spark

def cache_and_delete_files(df, partition_path=None):
    """
    Caches the DataFrame and deletes the files from the given partition path if necessary.

    :param df: The Spark DataFrame to be cached.
    :param partition_path: Optional path to the partition directory whose files may be deleted.
    :return: The cached DataFrame.
    """
    
    # Trigger Spark action and cache the DataFrame
    df.count()  # Trigger computation to ensure the DataFrame is materialized
    df.cache()  # Cache the DataFrame in memory
    
    # Optionally delete the files at the partition path
    if partition_path and os.path.exists(partition_path):
        # LOG.info(f"Deleting files at {partition_path}")
        shutil.rmtree(partition_path)  # Delete the directory and its contents
    else:
        # LOG.warning(f"Path {partition_path} does not exist or not provided, so nothing was deleted.")
        return df
    return df



def get_dataframe_schema(spark,partitioned_path: str):
    # Read the schema from the parquet file
    df = spark.read.parquet(partitioned_path)
    parquet_schema = df.schema
    
    # Return the schema directly
    return parquet_schema

def exceeds_data_loss_threshold(df: DataFrame, threshold_type: str) -> float:
    thresholds = {
        "very_strict": 1.0,
        "strict": 5.0,
        "moderate": 10.0
    }
    
    if threshold_type not in thresholds:
        raise ValueError(f"Invalid threshold_type: {threshold_type}. Choose from: {', '.join(thresholds.keys())}")

    # Check for null values in pickup_datetime
    total_count = df.count()
    null_count = df.filter(col("pickup_datetime").isNull()).count()
    print(null_count)
    if total_count == 0:
        return 0.0
    
    data_loss_percentage = (null_count / total_count) * 100
    threshold = thresholds[threshold_type]
    
    return data_loss_percentage >= threshold


# def exceeds_threshold(threshold_type: str, source_of_truth_df: DataFrame, merge_df: DataFrame) -> bool:
#     """
#     Computes the data loss percentage for pickup_datetime in merge_df compared to source_of_truth_df
#     and determines if it exceeds the given threshold.

#     Parameters:
#     - threshold_type (str): Type of threshold to check against ("very_strict", "strict", "moderate").
#     - source_of_truth_df (DataFrame): Source of truth DataFrame.
#     - merge_df (DataFrame): DataFrame to be merged into source_of_truth_df.

#     Returns:
#     - bool: True if data loss percentage in merge_df exceeds the threshold compared to the source of truth; otherwise, False.
#     """
    
#     # Define thresholds
#     thresholds = {
#         "very_strict": 1.0,
#         "strict": 5.0,
#         "moderate": 10.0
#     }
    
#     # Check if provided threshold_type is valid
#     if threshold_type not in thresholds:
#         print(f"Invalid threshold_type provided: {threshold_type}. Defaulting to 'strict' threshold.")
#         threshold_type = "strict"  # Set default value

#     # Get the threshold percentage
#     threshold_percentage = thresholds[threshold_type]

#     # Count rows in the source_of_truth_df and merge_df
#     source_total_count = source_of_truth_df.count()
#     merge_total_count = merge_df.count()

#     # Count null values in pickup_datetime in both DataFrames
#     source_null_count = source_of_truth_df.filter(col("pickup_datetime").isNull()).count()
#     merge_null_count = merge_df.filter(col("pickup_datetime").isNull()).count()

#     # Calculate data loss percentage in merge_df
#     merge_data_loss_percentage = (merge_null_count / merge_total_count) * 100 if merge_total_count > 0 else 0

#     # Debugging print statements
#     print(f"Threshold Type: {threshold_type}")
#     print(f"Threshold Percentage: {threshold_percentage}%")
#     print(f"Source Total Count: {source_total_count}")
#     print(f"Merge Total Count: {merge_total_count}")
#     print(f"Source Null Count: {source_null_count}")
#     print(f"Merge Null Count: {merge_null_count}")
#     print(f"Merge Data Loss Percentage: {merge_data_loss_percentage}%")

#     # Check if the data loss percentage exceeds the threshold
#     return merge_data_loss_percentage > threshold_percentage