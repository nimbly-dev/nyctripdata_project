DO $$
BEGIN
  IF EXISTS
    (
           SELECT 1
           FROM   pg_matviews
           WHERE  schemaname = 'dim'
           AND    matviewname = 'dim_cab_payment_type_summary_mv') THEN
    refresh materialized VIEW dim.dim_cab_payment_type_summary_mv;ELSE
    CREATE materialized VIEW dim.dim_cab_payment_type_summary_mv AS
    SELECT    pt.payment_type_description,
              count(ft.dwid)                                       AS total_trips,
              round(SUM(ft.fare_amount)::  NUMERIC, 2)             AS total_fare,
              round(avg(ft.fare_amount)::  NUMERIC, 2)             AS avg_fare,
              round(avg(ft.trip_distance)::NUMERIC, 2)             AS avg_distance,
              round(avg(ft.total_amount):: NUMERIC, 2)             AS avg_total_amount,
              count(ft.dwid) filter (WHERE ft.cab_type = 'yellow') AS yellow_cab_trips,
              count(ft.dwid) filter (WHERE ft.cab_type = 'green')  AS green_cab_trips
    FROM      fact.fact_tripdata ft
    left join dim.dim_payment_type pt
    ON        ft.payment_type = pt.payment_type_id
    WHERE     ft.payment_type IS NOT NULL
    GROUP BY  pt.payment_type_description
    ORDER BY  total_trips DESC;END IF;END $$;