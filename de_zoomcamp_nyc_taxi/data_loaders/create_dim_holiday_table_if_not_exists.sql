-- Create the dim.dim_holiday table
CREATE TABLE IF NOT EXISTS dim.dim_holiday (
    holiday_id SERIAL PRIMARY KEY,                 
    holiday_name VARCHAR(50) NOT NULL,             
    month INT NOT NULL,                           
    day_or_rule VARCHAR(20) NOT NULL,             
    holiday_type VARCHAR(30) NOT NULL,         
    nyc_observance BOOLEAN NOT NULL               
);

-- Insert holiday data
INSERT INTO dim.dim_holiday (holiday_name, month, day_or_rule, holiday_type, nyc_observance)
SELECT 'New Year''s Day', 1, '1', 'Federal', TRUE
WHERE NOT EXISTS (SELECT 1 FROM dim.dim_holiday WHERE holiday_name = 'New Year''s Day');

INSERT INTO dim.dim_holiday (holiday_name, month, day_or_rule, holiday_type, nyc_observance)
SELECT 'Lincoln''s Birthday', 2, '12', 'State (NYC)', TRUE
WHERE NOT EXISTS (SELECT 1 FROM dim.dim_holiday WHERE holiday_name = 'Lincoln''s Birthday');

INSERT INTO dim.dim_holiday (holiday_name, month, day_or_rule, holiday_type, nyc_observance)
SELECT 'Independence Day', 7, '4', 'Federal', TRUE
WHERE NOT EXISTS (SELECT 1 FROM dim.dim_holiday WHERE holiday_name = 'Independence Day');

INSERT INTO dim.dim_holiday (holiday_name, month, day_or_rule, holiday_type, nyc_observance)
SELECT 'Veterans Day', 11, '11', 'Federal', TRUE
WHERE NOT EXISTS (SELECT 1 FROM dim.dim_holiday WHERE holiday_name = 'Veterans Day');

INSERT INTO dim.dim_holiday (holiday_name, month, day_or_rule, holiday_type, nyc_observance)
SELECT 'Christmas Day', 12, '25', 'Federal', TRUE
WHERE NOT EXISTS (SELECT 1 FROM dim.dim_holiday WHERE holiday_name = 'Christmas Day');

INSERT INTO dim.dim_holiday (holiday_name, month, day_or_rule, holiday_type, nyc_observance)
SELECT 'Martin Luther King Jr. Day', 1, 'third_monday', 'Federal', TRUE
WHERE NOT EXISTS (SELECT 1 FROM dim.dim_holiday WHERE holiday_name = 'Martin Luther King Jr. Day');

INSERT INTO dim.dim_holiday (holiday_name, month, day_or_rule, holiday_type, nyc_observance)
SELECT 'Presidents'' Day', 2, 'third_monday', 'Federal', TRUE
WHERE NOT EXISTS (SELECT 1 FROM dim.dim_holiday WHERE holiday_name = 'Presidents'' Day');

INSERT INTO dim.dim_holiday (holiday_name, month, day_or_rule, holiday_type, nyc_observance)
SELECT 'Memorial Day', 5, 'last_monday', 'Federal', TRUE
WHERE NOT EXISTS (SELECT 1 FROM dim.dim_holiday WHERE holiday_name = 'Memorial Day');

INSERT INTO dim.dim_holiday (holiday_name, month, day_or_rule, holiday_type, nyc_observance)
SELECT 'Juneteenth National Independence Day', 6, '19', 'Federal', TRUE
WHERE NOT EXISTS (SELECT 1 FROM dim.dim_holiday WHERE holiday_name = 'Juneteenth National Independence Day');

INSERT INTO dim.dim_holiday (holiday_name, month, day_or_rule, holiday_type, nyc_observance)
SELECT 'Labor Day', 9, 'first_monday', 'Federal', TRUE
WHERE NOT EXISTS (SELECT 1 FROM dim.dim_holiday WHERE holiday_name = 'Labor Day');

INSERT INTO dim.dim_holiday (holiday_name, month, day_or_rule, holiday_type, nyc_observance)
SELECT 'Columbus Day', 10, 'second_monday', 'Federal', TRUE
WHERE NOT EXISTS (SELECT 1 FROM dim.dim_holiday WHERE holiday_name = 'Columbus Day');

INSERT INTO dim.dim_holiday (holiday_name, month, day_or_rule, holiday_type, nyc_observance)
SELECT 'Thanksgiving Day', 11, 'fourth_thursday', 'Federal', TRUE
WHERE NOT EXISTS (SELECT 1 FROM dim.dim_holiday WHERE holiday_name = 'Thanksgiving Day');
