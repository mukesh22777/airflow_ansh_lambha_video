CREATE DATABASE IF NOT EXISTS bronze;
CREATE DATABASE IF NOT EXISTS silver;
CREATE DATABASE IF NOT EXISTS gold;
CREATE DATABASE IF NOT EXISTS control;

CREATE TABLE IF NOT EXISTS control.etl_watermark_state (
  source_system       STRING,
  source_schema       STRING,
  source_table        STRING,
  watermark_column    STRING,
  last_success_value  TIMESTAMP,
  last_run_id         STRING,
  last_status         STRING,
  last_row_count      BIGINT,
  updated_at          TIMESTAMP
)
USING PARQUET;

CREATE TABLE IF NOT EXISTS bronze.customers_raw (
  payload             STRING,
  _ingest_ts          TIMESTAMP,
  _run_id             STRING,
  _source_table       STRING
)
USING PARQUET
PARTITIONED BY (ingest_date DATE);

CREATE TABLE IF NOT EXISTS bronze.orders_raw (
  payload             STRING,
  _ingest_ts          TIMESTAMP,
  _run_id             STRING,
  _source_table       STRING
)
USING PARQUET
PARTITIONED BY (ingest_date DATE);

CREATE TABLE IF NOT EXISTS silver.customers_curated (
  customerid          INT,
  customername        STRING,
  email               STRING,
  phone               STRING,
  modified_ts         TIMESTAMP,
  _ingest_ts          TIMESTAMP,
  _run_id             STRING,
  is_deleted          BOOLEAN
)
USING PARQUET;

CREATE TABLE IF NOT EXISTS silver.orders_curated (
  orderid             BIGINT,
  customerid          INT,
  orderamount         DECIMAL(18,2),
  orderstatus         STRING,
  modified_ts         TIMESTAMP,
  _ingest_ts          TIMESTAMP,
  _run_id             STRING,
  is_deleted          BOOLEAN
)
USING PARQUET;

CREATE TABLE IF NOT EXISTS gold.customer_daily_metrics (
  metric_date         DATE,
  total_orders        BIGINT,
  total_revenue       DECIMAL(18,2),
  active_customers    BIGINT,
  _run_id             STRING,
  _processed_ts       TIMESTAMP
)
USING PARQUET
PARTITIONED BY (metric_date);

CREATE TABLE IF NOT EXISTS gold.orders_daily_metrics (
  metric_date         DATE,
  total_orders        BIGINT,
  total_revenue       DECIMAL(18,2),
  active_customers    BIGINT,
  _run_id             STRING,
  _processed_ts       TIMESTAMP
)
USING PARQUET
PARTITIONED BY (metric_date);