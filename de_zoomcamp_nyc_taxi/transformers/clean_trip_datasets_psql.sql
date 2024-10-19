SELECT * FROM {{ df_1 }}
WHERE 
    pickup_datetime IS NOT NULL 
    AND dropoff_datetime IS NOT NULL 
    AND pu_location_id IS NOT NULL 
    AND do_location_id IS NOT NULL
UNION ALL
SELECT * FROM {{ df_2 }}
WHERE 
    pickup_datetime IS NOT NULL 
    AND dropoff_datetime IS NOT NULL 
    AND pu_location_id IS NOT NULL 
    AND do_location_id IS NOT NULL
UNION ALL
SELECT * FROM {{ df_3 }}
WHERE 
    pickup_datetime IS NOT NULL 
    AND dropoff_datetime IS NOT NULL 
    AND pu_location_id IS NOT NULL 
    AND do_location_id IS NOT NULL