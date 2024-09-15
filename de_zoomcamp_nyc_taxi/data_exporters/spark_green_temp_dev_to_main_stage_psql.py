from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from sqlalchemy import create_engine, text
import os
from de_zoomcamp_nyc_taxi.utils.sql.sql_util import execute_function_on_postgres
from de_zoomcamp_nyc_taxi.utils.spark.spark_util import get_spark_session
from de_zoomcamp_nyc_taxi.model.schema.green_tripdata import GreenTripDataSchema

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

SPARK_PARTITION_FILES_DIR = os.getenv('SPARK_PARTITION_FILES_DIR', '/opt/spark/spark-warehouse/partitioned')

@data_exporter
def export_data(data, *args, **kwargs):
    LOG = kwargs.get('logger')
    year = kwargs.get('year')
    month = kwargs.get('month')
    spark_mode = kwargs.get('spark_mode', 'local')  # Default to 'local' if not provided

    # Extract configuration parameters
    stage_database_url = kwargs['configuration'].get('stage_databaseurl')
    stage_db_name = kwargs['configuration'].get('stage_dbname')
    tripdata_type = kwargs['tripdata_type']

    target_date = f"{year}-{month:02d}-01"
    base_stage_path = os.path.join(SPARK_PARTITION_FILES_DIR, f'{tripdata_type}/pq/stage/{year}-{month:02d}')
    partition_path = os.path.join(base_stage_path, f'year={year}', f'month={month}')

    staging_table_name = f'{tripdata_type}_staging'
    temp_table_name = f'{tripdata_type}_temp_staging'

    # Create SQLAlchemy engine for the staging database
    stage_engine_url = f'postgresql://postgres:postgres@{stage_database_url}/{stage_db_name}'
    stage_engine = create_engine(stage_engine_url)

    # Configure Spark session with PostgreSQL JDBC driver
    cluster_additional_configs = {
        'spark.jars': '/opt/spark/third-party-jars/postgresql-42.2.24.jar',
        'spark.driver.extraClassPath': '/opt/spark/third-party-jars/postgresql-42.2.24.jar'
    }

    spark = get_spark_session(
        mode=spark_mode,
        additional_configs=cluster_additional_configs,
        appname='spark_yellow_temp_dev_to_main_stage_psql'
    )
    LOG.info("Spark session initiated.")
    
    try:
        # Read all Parquet files in the directory
        LOG.info(f"Reading Parquet files from directory: {partition_path}.")
        green_tripdata_schema = GreenTripDataSchema(spark_session=spark)
        df_spark = spark.read.schema(green_tripdata_schema.get_dataframe_schema(partition_path)).parquet(partition_path)

        LOG.info(f"Creating partition for date: {target_date}")
        partition_creation_query = f"SELECT public.create_partition_if_not_exists('{staging_table_name}', '{target_date}');"
        execute_function_on_postgres(stage_engine_url, partition_creation_query)

        # Write data to a temporary table
        LOG.info(f"Writing data to temporary table: {temp_table_name}.")
        df_spark.write \
            .format("jdbc") \
            .option("url", f"jdbc:postgresql://{stage_database_url}/{stage_db_name}") \
            .option("dbtable", f"temp.{temp_table_name}") \
            .option("user", "postgres") \
            .option("password", "postgres") \
            .option("driver", "org.postgresql.Driver") \
            .mode("overwrite") \
            .save()

        # Perform upsert from temporary table to staging table with partition filtering
        LOG.info("Performing upsert from temporary table to staging table.")
        upsert_query = text(f"""
            INSERT INTO public.{staging_table_name} (dwid, vendor_id, pickup_datetime, dropoff_datetime, passenger_count, trip_distance, ratecode_id, store_and_fwd_flag, pu_location_id, do_location_id, payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount, congestion_surcharge, airport_fee)
            SELECT dwid, vendor_id, pickup_datetime, dropoff_datetime, passenger_count, trip_distance, ratecode_id, store_and_fwd_flag, pu_location_id, do_location_id, payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount, congestion_surcharge, airport_fee
            FROM temp.{temp_table_name}
            WHERE pickup_datetime >= '{year}-{month:02d}-01 00:00:00' AND pickup_datetime < '{year}-{month:02d}-01 23:59:59'
            ON CONFLICT (dwid, pickup_datetime, dropoff_datetime)
            DO UPDATE SET
                vendor_id = EXCLUDED.vendor_id,
                passenger_count = EXCLUDED.passenger_count,
                trip_distance = EXCLUDED.trip_distance,
                ratecode_id = EXCLUDED.ratecode_id,
                store_and_fwd_flag = EXCLUDED.store_and_fwd_flag,
                pu_location_id = EXCLUDED.pu_location_id,
                do_location_id = EXCLUDED.do_location_id,
                payment_type = EXCLUDED.payment_type,
                fare_amount = EXCLUDED.fare_amount,
                extra = EXCLUDED.extra,
                mta_tax = EXCLUDED.mta_tax,
                tip_amount = EXCLUDED.tip_amount,
                tolls_amount = EXCLUDED.tolls_amount,
                improvement_surcharge = EXCLUDED.improvement_surcharge,
                total_amount = EXCLUDED.total_amount,
                congestion_surcharge = EXCLUDED.congestion_surcharge,
                airport_fee = EXCLUDED.airport_fee;
        """)
        with stage_engine.connect() as conn:
            conn.execute(upsert_query)

        LOG.info("Data upsert completed successfully.")

    except Exception as e:
        LOG.error(f"Error during data upsert: {e}")
        raise

    finally:
        # Drop the temporary table
        LOG.info(f"Dropping temporary table: {temp_table_name}")
        with stage_engine.connect() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS temp.{temp_table_name};"))

        LOG.info("Upsert data process completed.")
        spark.stop()
