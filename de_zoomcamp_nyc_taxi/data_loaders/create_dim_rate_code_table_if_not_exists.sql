CREATE TABLE IF NOT EXISTS dim.dim_rate_code (
    ratecode_id INT PRIMARY KEY,
    rate_code_description VARCHAR(50)
);

INSERT INTO dim.dim_rate_code (ratecode_id, rate_code_description)
SELECT 1, 'Standard Rate' WHERE NOT EXISTS (SELECT 1 FROM dim.dim_rate_code WHERE ratecode_id = 1);
INSERT INTO dim.dim_rate_code (ratecode_id, rate_code_description)
SELECT 2, 'JFK' WHERE NOT EXISTS (SELECT 1 FROM dim.dim_rate_code WHERE ratecode_id = 2);
INSERT INTO dim.dim_rate_code (ratecode_id, rate_code_description)
SELECT 3, 'Newark' WHERE NOT EXISTS (SELECT 1 FROM dim.dim_rate_code WHERE ratecode_id = 3);
INSERT INTO dim.dim_rate_code (ratecode_id, rate_code_description)
SELECT 4, 'Nassau or Westchester' WHERE NOT EXISTS (SELECT 1 FROM dim.dim_rate_code WHERE ratecode_id = 4);
INSERT INTO dim.dim_rate_code (ratecode_id, rate_code_description)
SELECT 5, 'Negotiated Fare' WHERE NOT EXISTS (SELECT 1 FROM dim.dim_rate_code WHERE ratecode_id = 5);
INSERT INTO dim.dim_rate_code (ratecode_id, rate_code_description)
SELECT 6, 'Group Ride' WHERE NOT EXISTS (SELECT 1 FROM dim.dim_rate_code WHERE ratecode_id = 6);
