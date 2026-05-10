CREATE TABLE IF NOT EXISTS telecom_customer_warehouse (
    customer_id INT PRIMARY KEY,
    full_name VARCHAR(256),
    service_tier VARCHAR(50),
    region_code CHAR(1),
    score INT,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
