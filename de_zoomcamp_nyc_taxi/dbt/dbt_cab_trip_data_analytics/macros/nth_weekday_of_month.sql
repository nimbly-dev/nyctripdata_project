{% macro nth_weekday_of_month(year, month, day_or_rule) %}
CASE
    -- If day_or_rule is numeric, then treat it as a day-of-month.
    WHEN LOWER(TRIM({{ day_or_rule }})) ~ '^[0-9]+$' THEN
        MAKE_DATE({{ year }}, {{ month }}, CAST({{ day_or_rule }} AS INT))
    -- If the day_or_rule follows a rule pattern (e.g. 'first_monday', 'last_friday', etc.)
    WHEN EXISTS (
        SELECT 1
        FROM regexp_matches(LOWER(TRIM({{ day_or_rule }})), '^(first|second|third|fourth|last)_(sunday|monday|tuesday|wednesday|thursday|friday|saturday)$')
    ) THEN
        CASE
            WHEN SPLIT_PART(LOWER(TRIM({{ day_or_rule }})), '_', 1) = 'last' THEN
                MAKE_DATE({{ year }}, {{ month }} + 1, 1)
                - INTERVAL '1 day'
                - (
                    (
                      (EXTRACT(DOW FROM (MAKE_DATE({{ year }}, {{ month }} + 1, 1) - INTERVAL '1 day'))::INT
                       - (
                            CASE SPLIT_PART(LOWER(TRIM({{ day_or_rule }})), '_', 2)
                                WHEN 'sunday' THEN 0
                                WHEN 'monday' THEN 1
                                WHEN 'tuesday' THEN 2
                                WHEN 'wednesday' THEN 3
                                WHEN 'thursday' THEN 4
                                WHEN 'friday' THEN 5
                                WHEN 'saturday' THEN 6
                                ELSE 0
                            END
                       )
                      + 7
                      ) % 7
                    ) * INTERVAL '1 day'
                )
            ELSE
                MAKE_DATE({{ year }}, {{ month }}, 1)
                + (
                    (
                      (
                        CASE SPLIT_PART(LOWER(TRIM({{ day_or_rule }})), '_', 2)
                          WHEN 'sunday' THEN 0
                          WHEN 'monday' THEN 1
                          WHEN 'tuesday' THEN 2
                          WHEN 'wednesday' THEN 3
                          WHEN 'thursday' THEN 4
                          WHEN 'friday' THEN 5
                          WHEN 'saturday' THEN 6
                          ELSE 0
                        END
                        - EXTRACT(DOW FROM MAKE_DATE({{ year }}, {{ month }}, 1)) + 7
                      ) % 7
                    ) * INTERVAL '1 day'
                    + (
                        (CAST(
                            CASE SPLIT_PART(LOWER(TRIM({{ day_or_rule }})), '_', 1)
                              WHEN 'first' THEN 1
                              WHEN 'second' THEN 2
                              WHEN 'third' THEN 3
                              WHEN 'fourth' THEN 4
                              ELSE 0
                            END AS INT)
                         - 1
                        ) * 7 * INTERVAL '1 day'
                    )
                )
        END
    ELSE
        NULL
END
{% endmacro %}
