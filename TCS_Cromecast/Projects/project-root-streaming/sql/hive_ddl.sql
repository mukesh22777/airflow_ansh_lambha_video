CREATE DATABASE IF NOT EXISTS {{database}};

CREATE TABLE IF NOT EXISTS {{database}}.Train_bronze (
  id INT,
  name STRING,
  department STRING,
  salary DOUBLE,
  age INT,
  city STRING,
  timestamp TIMESTAMP,
  Load_date TIMESTAMP
)
USING PARQUET
PARTITIONED BY (department);

CREATE TABLE IF NOT EXISTS {{database}}.Train_silver (
  id INT,
  name STRING,
  department STRING,
  salary DOUBLE,
  age INT,
  city STRING,
  timestamp TIMESTAMP,
  Load_date TIMESTAMP
)
USING PARQUET
PARTITIONED BY (department);

CREATE TABLE IF NOT EXISTS {{database}}.Train_gold (
  department STRING,
  city STRING,
  employee_count BIGINT,
  avg_salary DOUBLE,
  max_salary DOUBLE,
  min_salary DOUBLE,
  Load_date TIMESTAMP
)
USING PARQUET
PARTITIONED BY (department);
