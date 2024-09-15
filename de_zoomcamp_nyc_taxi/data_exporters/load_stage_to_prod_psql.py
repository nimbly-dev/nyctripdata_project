from sqlalchemy import func
from de_zoomcamp_nyc_taxi.utils.sql.sql_util import run_sql_on_postgres, execute_function_on_postgres, check_partition_exists

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

@data_exporter
def export_data(data, *args, **kwargs):
    year = kwargs['year']
    month = kwargs['month']

    # Format target date and table name
    tripdata_type = kwargs['configuration'].get('tripdata_type')
    target_date = f"{year}-{month:02d}-01"
    partition_name = f"{tripdata_type}_staging_{year}_{month:02d}"
    staging_table = f"{tripdata_type}_{year}{month:02d}_stage_temp"
    engine_name = 'postgresql://postgres:postgres@nyc-taxi-postgres:5432/nyc_taxi_data'
    prod_table_name = f'{tripdata_type}_prod'

    partition_creation_query = func.create_partition_if_not_exists(target_date)
    # print(f"Executing partition creation query for date: {target_date}")

    # Execute the partition creation function
    execute_function_on_postgres(engine_name, partition_creation_query)


    upsert_query = f"""
    INSERT INTO '{prod_table_name}' (
        dwid,
        vendor_id,
        tpep_pickup_datetime,
        tpep_dropoff_datetime,
        passenger_count,
        trip_distance,
        ratecode_id,
        store_and_fwd_flag,
        pu_location_id,
        do_location_id,
        payment_type,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        total_amount,
        congestion_surcharge,
        airport_fee
    )
    SELECT
        s.dwid,
        s.vendor_id,
        s.tpep_pickup_datetime,
        s.tpep_dropoff_datetime,
        s.passenger_count,
        s.trip_distance,
        s.ratecode_id,
        s.store_and_fwd_flag,
        s.pu_location_id,
        s.do_location_id,
        s.payment_type,
        s.fare_amount,
        s.extra,
        s.mta_tax,
        s.tip_amount,
        s.tolls_amount,
        s.improvement_surcharge,
        s.total_amount,
        s.congestion_surcharge,
        s.airport_fee
    FROM public.{staging_table} s
    WHERE s.tpep_pickup_datetime >= '{target_date}'
    AND s.tpep_pickup_datetime < '{target_date}'::date + interval '1 month'
    ON CONFLICT (dwid, tpep_pickup_datetime, tpep_dropoff_datetime)
    DO UPDATE SET
        vendor_id = EXCLUDED.vendor_id,
        tpep_pickup_datetime = EXCLUDED.tpep_pickup_datetime,
        tpep_dropoff_datetime = EXCLUDED.tpep_dropoff_datetime,
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
    """


    run_sql_on_postgres(upsert_query, engine_name)
