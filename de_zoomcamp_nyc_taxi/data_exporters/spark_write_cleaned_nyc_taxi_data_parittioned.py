if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

from de_zoomcamp_nyc_taxi.model.schema.yellow_tripdata import YellowTripDataSchema
from de_zoomcamp_nyc_taxi.utils.spark.spark_util import get_sparksession_for_codeblock, cache_and_delete_files
from pyspark.sql.functions import year as pyspark_year, month as pyspark_month

import os

SPARK_PARTITION_FILES_DIR = os.getenv('SPARK_PARTITION_FILES_DIR', '/opt/spark/spark-warehouse/partitioned')
SPARK_WAREHOUSE_DIR = os.getenv('SPARK_WAREHOUSE_DIR', '/opt/spark/spark-warehouse')

@data_exporter
def export_data(data, *args, **kwargs):
    year = kwargs['year']
    month = kwargs['month']

    # Where the parquet files are contained
    partitioned_path = os.path.join(SPARK_PARTITION_FILES_DIR, f'yellow_tripdata/pq/stage/year={year}/month={month}')
    clean_path = os.path.join(SPARK_PARTITION_FILES_DIR, f'yellow_tripdata/pq/clean')

    spark = get_sparksession_for_codeblock(args, kwargs)

    yellow_tripdata_schema = YellowTripDataSchema(spark_session=spark)

    df = spark.read.schema(yellow_tripdata_schema.get_dataframe_schema(partitioned_path)).parquet(partitioned_path)

    df = df.withColumn("year", pyspark_year(df["tpep_pickup_datetime"])) \
            .withColumn("month", pyspark_month(df["tpep_pickup_datetime"]))


    df.write.partitionBy("year", "month").parquet(clean_path, mode="overwrite")

    return df


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'



