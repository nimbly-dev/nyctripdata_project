from sqlalchemy import create_engine, text
import pandas as pd
import os
from mage_ai.data_preparation.decorators import data_exporter
from de_zoomcamp_nyc_taxi.utils.sql.sql_util import execute_function_on_postgres, run_sql_on_postgres

SPARK_PARTITION_FILES_DIR = os.getenv('SPARK_PARTITION_FILES_DIR', '/opt/spark/spark-warehouse/partitioned')

@data_exporter
def export_data(data, *args, **kwargs):
    LOG = kwargs.get('logger')
    year = kwargs.get('year')
    month = kwargs.get('month')

    # Extract configuration parameters
    data_warehouse_dir = kwargs['configuration'].get('data_warehouse_dir')
    stage_database_url = kwargs['configuration'].get('stage_databaseurl')
    stage_db_name = kwargs['configuration'].get('stage_dbname')
    tripdata_type = kwargs['configuration'].get('tripdata_type')

    target_date = f"{year}-{month:02d}-01"
    parquet_path = f"{SPARK_PARTITION_FILES_DIR}/{tripdata_type}/pq/stage/{year}-{month:02d}/year={year}/month={month}"

    staging_table_name = f'{tripdata_type}_staging'
    temp_table_name = f'{tripdata_type}_temp_staging'

    # Create SQLAlchemy engine for the staging database
    stage_engine_url = f'postgresql://postgres:postgres@{stage_database_url}/{stage_db_name}'
    stage_engine = create_engine(stage_engine_url)

    try:
        # Read Parquet files
        LOG.info(f"Reading Parquet files from {parquet_path}.")
        df = pd.read_parquet(parquet_path)
        
        # Create partition if needed
        LOG.info(f"Creating partition for date: {target_date}")
        partition_creation_query = f"SELECT public.create_partition_if_not_exists('{staging_table_name}', '{target_date}');"
        execute_function_on_postgres(stage_engine_url, partition_creation_query)

        # Create a temporary table to load data
        LOG.info(f"Creating temporary table: {temp_table_name}")
        create_temp_table_query = text(f"""
            DROP TABLE IF EXISTS temp.{temp_table_name};
            CREATE TABLE temp.{temp_table_name} AS TABLE temp.{staging_table_name} WITH NO DATA;
        """)
        with stage_engine.connect() as conn:
            conn.execute(create_temp_table_query)

        # Insert data into the temporary table
        LOG.info("Inserting data into the temporary table.")
        df.to_sql(temp_table_name, stage_engine, if_exists='append', index=False, method='multi')

        # Perform upsert from temporary table to staging table
        LOG.info("Performing upsert from temporary table to staging table.")
        upsert_query = text(f"""
            INSERT INTO {staging_table_name} (dwid, vendor_id, pickup_datetime, dropoff_datetime, passenger_count, trip_distance, ratecode_id, store_and_fwd_flag, pu_location_id, do_location_id, payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount, congestion_surcharge, airport_fee)
            SELECT dwid, vendor_id, pickup_datetime, dropoff_datetime, passenger_count, trip_distance, ratecode_id, store_and_fwd_flag, pu_location_id, do_location_id, payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount, congestion_surcharge, airport_fee
            FROM temp.{temp_table_name}
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

        # Drop the temporary table
        LOG.info(f"Dropping temporary table: {temp_table_name}")
        with stage_engine.connect() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS temp.{temp_table_name};"))

        LOG.info("Data upsert completed successfully.")

    except Exception as e:
        LOG.error(f"Error during data upsert: {e}")
        raise

    finally:
        LOG.info("Upsert data process completed.")
