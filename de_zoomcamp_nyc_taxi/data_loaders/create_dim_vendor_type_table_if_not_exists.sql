CREATE TABLE IF NOT EXISTS dim.dim_vendor (
    vendor_id INT PRIMARY KEY,
    vendor_name VARCHAR(50)
);

INSERT INTO dim.dim_vendor (vendor_id, vendor_name)
SELECT 1, 'Creative Mobile Technologies (CMT)' 
WHERE NOT EXISTS (SELECT 1 FROM dim.dim_vendor WHERE vendor_id = 1);

INSERT INTO dim.dim_vendor (vendor_id, vendor_name)
SELECT 2, 'Verifone Inc.' 
WHERE NOT EXISTS (SELECT 1 FROM dim.dim_vendor WHERE vendor_id = 2);