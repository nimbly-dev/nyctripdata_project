CREATE TABLE IF NOT EXISTS dim.dim_payment_type (
    payment_type_id INT PRIMARY KEY,                
    payment_type_description VARCHAR(50)             
);

INSERT INTO dim.dim_payment_type (payment_type_id, payment_type_description)
SELECT 1, 'Credit card' WHERE NOT EXISTS (SELECT 1 FROM dim.dim_payment_type WHERE payment_type_id = 1);
INSERT INTO dim.dim_payment_type (payment_type_id, payment_type_description)
SELECT 2, 'Cash' WHERE NOT EXISTS (SELECT 1 FROM dim.dim_payment_type WHERE payment_type_id = 2);
INSERT INTO dim.dim_payment_type (payment_type_id, payment_type_description)
SELECT 3, 'No charge' WHERE NOT EXISTS (SELECT 1 FROM dim.dim_payment_type WHERE payment_type_id = 3);
INSERT INTO dim.dim_payment_type (payment_type_id, payment_type_description)
SELECT 4, 'Dispute' WHERE NOT EXISTS (SELECT 1 FROM dim.dim_payment_type WHERE payment_type_id = 4);
INSERT INTO dim.dim_payment_type (payment_type_id, payment_type_description)
SELECT 5, 'Unknown' WHERE NOT EXISTS (SELECT 1 FROM dim.dim_payment_type WHERE payment_type_id = 5);
INSERT INTO dim.dim_payment_type (payment_type_id, payment_type_description)
SELECT 6, 'Voided trip' WHERE NOT EXISTS (SELECT 1 FROM dim.dim_payment_type WHERE payment_type_id = 6);
