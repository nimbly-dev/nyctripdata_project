DO $$
BEGIN
  IF EXISTS
    (
           SELECT 1
           FROM   pg_matviews
           WHERE  schemaname = 'dim'
           AND    matviewname = 'dim_rate_code_summary_mv' ) THEN
    refresh materialized VIEW dim.dim_rate_code_summary_mv;ELSE
    CREATE materialized VIEW dim.dim_rate_code_summary_mv AS
    SELECT    coalesce(rc.rate_code_description, 'Unknown')        AS rate_code_description,
              count(ft.dwid)                                       AS total_trips,
              round(avg(ft.fare_amount)::  NUMERIC, 2)             AS avg_fare,
              round(avg(ft.trip_distance)::NUMERIC, 2)             AS avg_distance,
              round(avg(ft.total_amount):: NUMERIC, 2)             AS avg_total_amount,
              count(ft.dwid) filter (WHERE ft.cab_type = 'yellow') AS yellow_cab_trips,
              count(ft.dwid) filter (WHERE ft.cab_type = 'green')  AS green_cab_trips
    FROM      fact.fact_tripdata ft
    left join dim.dim_rate_code rc
    ON        ft.ratecode_id = rc.ratecode_id
    WHERE     ft.cab_type IN ('yellow',
                              'green')
    GROUP BY  coalesce(rc.rate_code_description, 'Unknown')
    ORDER BY  total_trips DESC;END IF;END $$;