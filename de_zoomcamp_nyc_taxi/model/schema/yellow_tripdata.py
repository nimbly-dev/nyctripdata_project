from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import types as T
from pyspark.sql.functions import col

class YellowTripDataSchema:
    def __init__(self, spark_session: SparkSession):
        self.spark = spark_session

    def get_base_schema(self) -> T.StructType:
        return T.StructType([
            T.StructField("VendorID", T.IntegerType(), nullable=True),
            T.StructField("tpep_pickup_datetime", T.TimestampType(), nullable=False),
            T.StructField("tpep_dropoff_datetime", T.TimestampType(), nullable=False),
            T.StructField("passenger_count", T.IntegerType(), nullable=True),
            T.StructField("trip_distance", T.FloatType(), nullable=True),
            T.StructField("RatecodeID", T.IntegerType(), nullable=True),
            T.StructField("store_and_fwd_flag", T.StringType(), nullable=True),
            T.StructField("PULocationID", T.IntegerType(), nullable=True),
            T.StructField("DOLocationID", T.IntegerType(), nullable=True),
            T.StructField("payment_type", T.IntegerType(), nullable=True),
            T.StructField("fare_amount", T.FloatType(), nullable=True),
            T.StructField("extra", T.FloatType(), nullable=True),
            T.StructField("mta_tax", T.FloatType(), nullable=True),
            T.StructField("tip_amount", T.FloatType(), nullable=True),
            T.StructField("tolls_amount", T.FloatType(), nullable=True),
            T.StructField("improvement_surcharge", T.FloatType(), nullable=True),
            T.StructField("total_amount", T.FloatType(), nullable=True),
            T.StructField("congestion_surcharge", T.FloatType(), nullable=True),
            T.StructField("airport_fee", T.FloatType(), nullable=True),
        ])

    def cast_columns(self, df: DataFrame) -> DataFrame:
        schema = self.get_base_schema()
        for field in schema.fields:
            if field.name in df.columns:
                df = df.withColumn(field.name, col(field.name).cast(field.dataType))
        return df