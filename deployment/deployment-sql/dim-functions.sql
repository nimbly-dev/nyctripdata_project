-- FUNCTION: dim.nth_weekday_of_month(integer, integer, text)

-- DROP FUNCTION IF EXISTS dim.nth_weekday_of_month(integer, integer, text);

CREATE OR REPLACE FUNCTION dim.nth_weekday_of_month(
	year integer,
	month integer,
	day_or_rule text)
    RETURNS date
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE
    weekday INT;
    occurrence INT;
    first_of_month DATE;
    first_weekday_offset INT;
    occurrence_part TEXT;
    weekday_name TEXT;
BEGIN
    -- Convert input to lowercase and trim
    day_or_rule := LOWER(TRIM(day_or_rule));

    -- Log the original input for debugging
    RAISE NOTICE 'Processing rule: %', day_or_rule;

    -- Check if the format contains an underscore
    IF POSITION('_' IN day_or_rule) > 0 THEN
        -- Split day_or_rule by the underscore
        occurrence_part := SPLIT_PART(day_or_rule, '_', 1);
        weekday_name := SPLIT_PART(day_or_rule, '_', 2);
    ELSE
        -- Raise an exception if the format does not match expected pattern
        RAISE EXCEPTION 'Unsupported format in rule: %, expected underscore-separated format', day_or_rule;
    END IF;

    -- Determine the occurrence
    occurrence := CASE occurrence_part
        WHEN 'first' THEN 1
        WHEN 'second' THEN 2
        WHEN 'third' THEN 3
        WHEN 'fourth' THEN 4
        WHEN 'last' THEN 5
        ELSE NULL
    END;

    -- Log parsed occurrence for debugging
    RAISE NOTICE 'Parsed occurrence: %, from rule: %', occurrence, day_or_rule;

    -- Convert the weekday name to an integer
    weekday := CASE weekday_name
        WHEN 'sunday' THEN 0
        WHEN 'monday' THEN 1
        WHEN 'tuesday' THEN 2
        WHEN 'wednesday' THEN 3
        WHEN 'thursday' THEN 4
        WHEN 'friday' THEN 5
        WHEN 'saturday' THEN 6
        ELSE NULL
    END;

    -- Log parsed weekday for debugging
    RAISE NOTICE 'Parsed weekday: %, from rule: %', weekday, day_or_rule;

    -- Check if parsing was successful
    IF occurrence IS NULL OR weekday IS NULL THEN
        RAISE EXCEPTION 'Unsupported occurrence or weekday in rule: %', day_or_rule;
    END IF;

    -- Calculate the base date as the first day of the given month and year
    first_of_month := make_date(year, month, 1);
    first_weekday_offset := (weekday - EXTRACT(DOW FROM first_of_month) + 7) % 7;

    -- Handle the "last" occurrence separately
    IF occurrence = 5 THEN
        RETURN (make_date(year, month + 1, 1) - interval '1 day')::date - 
               (EXTRACT(DOW FROM (make_date(year, month + 1, 1) - interval '1 day'))::integer - weekday + 7) % 7;
    ELSE
        -- Handle "first", "second", "third", and "fourth"
        RETURN first_of_month + first_weekday_offset + (occurrence - 1) * 7;
    END IF;
END;
$BODY$;