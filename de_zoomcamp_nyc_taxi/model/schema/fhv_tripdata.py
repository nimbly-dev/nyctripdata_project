from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import types as T
from pyspark.sql.functions import col

class FHVTripDataSchema:
    def __init__(self, spark_session: SparkSession):
        self.spark = spark_session

    def get_base_schema(self) -> T.StructType:
        return T.StructType([
            T.StructField("dispatching_base_num", T.StringType(), nullable=True),
            T.StructField("pickup_datetime", T.TimestampType(), nullable=False),
            T.StructField("dropOff_datetime", T.TimestampType(), nullable=False),
            T.StructField("PUlocationID", T.IntegerType(), nullable=True),
            T.StructField("DOlocationID", T.IntegerType(), nullable=True),
            T.StructField("SR_Flag", T.IntegerType(), nullable=True),
            T.StructField("Affiliated_base_number", T.StringType(), nullable=True),
        ])

    def cast_columns(self, df: DataFrame) -> DataFrame:
        schema = self.get_base_schema()
        for field in schema.fields:
            if field.name in df.columns:
                df = df.withColumn(field.name, col(field.name).cast(field.dataType))
        return df
