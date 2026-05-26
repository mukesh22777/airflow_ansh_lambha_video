IF DB_ID('medallion_db') IS NULL
BEGIN
    CREATE DATABASE medallion_db;
END
GO

USE medallion_db;
GO

IF OBJECT_ID('dbo.customer_orders', 'U') IS NOT NULL
BEGIN
    DROP TABLE dbo.customer_orders;
END
GO

CREATE TABLE dbo.customer_orders (
    order_id INT PRIMARY KEY,
    customer_id INT,
    city VARCHAR(100),
    amount DECIMAL(12,2),
    status VARCHAR(50),
    event_time DATETIME2,
    last_updated DATETIME2
);
GO

INSERT INTO dbo.customer_orders (order_id, customer_id, city, amount, status, event_time, last_updated) VALUES
(1, 101, 'Delhi', 120.00, 'active', '2026-05-06T09:10:00', '2026-05-07T20:00:00'),
(2, 102, 'Mumbai', 220.00, 'active', '2026-05-06T10:11:00', '2026-05-07T20:01:00'),
(3, 103, 'Pune', NULL, 'inactive', '2026-05-06T11:12:00', '2026-05-07T20:02:00'),
(4, 104, 'Chennai', 340.00, 'active', '2026-05-06T12:13:00', '2026-05-07T20:03:00'),
(5, 105, 'Kolkata', 450.00, 'active', '2026-05-06T13:14:00', '2026-05-07T20:04:00');
GO