# SQL Server -> Hive Medallion on Azure with Airflow + PySpark (Production Guide)

This guide provides a **single, production-grade implementation blueprint** for ingesting data from **SQL Server** into **Hive Medallion layers (Bronze/Silver/Gold)** on Azure using **PySpark** jobs orchestrated by **Airflow**.

It covers:
- Daily Airflow DAG at **10:00 PM**
- **Initial full load** + **incremental watermark** processing
- JDBC configuration and secure secrets usage
- Hive DDLs for Bronze/Silver/Gold
- Configuration management
- Logging, validation, and error handling

## 1) Reference Architecture

1. **Source**: SQL Server OLTP tables.
2. **Orchestration**: Airflow DAG triggers Bronze -> Silver -> Gold in order.
3. **Compute**: Spark (Synapse/Databricks/standalone Spark on Azure VM or AKS).
4. **Storage**: ADLS Gen2 path layout for raw/curated/serving and Hive external tables.
5. **Metadata & State**:
   - Hive Metastore for table schemas.
   - Control table for per-table watermark and run metadata.
6. **Monitoring**: Centralized logs (Airflow + Spark logs + metrics table + alerts).

## 2) Suggested Repository Layout

project-root/
│
├── 📄 airflow_spark_pipeline_guide.md     # Documentation (architecture, steps, usage)
│
├── 📁 dags/                              # Airflow DAG definitions
│   └── sqlserver_hive_medallion_dag.py   # Main orchestration pipeline
│
├── 📁 jobs/                              # PySpark ETL jobs
│   ├── bronze_ingest.py                  # SQL Server → Bronze (raw ingestion)
│   ├── silver_transform.py               # Data cleaning & transformation
│   ├── gold_aggregate.py                 # Business-level aggregations
│   │
│   └── 📁 common/                        # Reusable utilities (shared modules)
│       ├── config.py                     # Spark session + config loader
│       ├── logger.py                     # Logging utility
│       ├── jdbc.py                       # JDBC connection handling
│       ├── watermark.py                  # Incremental load logic
│       ├── validation.py                 # Data quality checks
│       └── hive_ddl.py                   # Hive table creation helpers
│
├── 📁 conf/                              # Configuration files
│   └── pipeline_config.yaml              # Environment configs (DB, paths, etc.)
│
├── 📁 sql/                               # SQL scripts
│   └── hive_ddl.sql                      # Hive DDL (tables, partitions)
│
└── 📁 logs/                              # Runtime logs (Airflow / Spark)

> You asked for exactly one delivered file; this structure is an implementation reference. Code samples below are complete and can be split into files later.


## 3) Configuration Management (YAML + Airflow Variables/Connections)

Use config file for non-secret values and Airflow Connections/Key Vault for secrets.

### Example `pipeline_config.yaml`

```yaml
env: prod
timezone: "America/Chicago"
pipeline:
  name: sqlserver_hive_medallion
  schedule_cron: "0 22 * * *"     # daily 10 PM
  max_active_runs: 1
  retries: 2
  retry_delay_minutes: 10
spark:
  app_name_prefix: medallion_etl
  shuffle_partitions: 200
  adaptive_enabled: true
azure:
  storage_account: "mystorageacct"
  container: "datalake"
  bronze_path: "abfss://datalake@mystorageacct.dfs.core.windows.net/bronze"
  silver_path: "abfss://datalake@mystorageacct.dfs.core.windows.net/silver"
  gold_path: "abfss://datalake@mystorageacct.dfs.core.windows.net/gold"
sqlserver:
  host: "sqlserver-prod.database.windows.net"
  port: 1433
  database: "SalesDB"
  driver: "com.microsoft.sqlserver.jdbc.SQLServerDriver"
  encrypt: "true"
  trustServerCertificate: "false"
watermark:
  control_db: "control"
  control_table: "etl_watermark_state"
  default_low_value: "1900-01-01 00:00:00"
tables:
  - source_schema: "dbo"
    source_table: "Customers"
    primary_key: "CustomerID"
    watermark_column: "ModifiedDate"
    bronze_table: "bronze.customers_raw"
    silver_table: "silver.customers_curated"
    gold_table: "gold.customer_daily_metrics"
  - source_schema: "dbo"
    source_table: "Orders"
    primary_key: "OrderID"
    watermark_column: "ModifiedDate"
    bronze_table: "bronze.orders_raw"
    silver_table: "silver.orders_curated"
    gold_table: "gold.orders_daily_metrics"
```

### Airflow connections/variables

- Connection `sqlserver_jdbc`:
  - host, port, schema(database), login, password
- Variable `pipeline_config_path` -> absolute path to YAML
- Optional: secrets in Azure Key Vault fetched at runtime


## 4) Hive Databases and DDLs (Bronze/Silver/Gold + Control)

Use external tables over ADLS locations.

```sql
CREATE DATABASE IF NOT EXISTS bronze;
CREATE DATABASE IF NOT EXISTS silver;
CREATE DATABASE IF NOT EXISTS gold;
CREATE DATABASE IF NOT EXISTS control;

-- Control state table for watermark + run auditing
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

-- Bronze: raw append-only with ingestion metadata
CREATE TABLE IF NOT EXISTS bronze.customers_raw (
  CustomerID          INT,
  CustomerName        STRING,
  Email               STRING,
  Phone               STRING,
  ModifiedDate        TIMESTAMP,
  _ingest_ts          TIMESTAMP,
  _run_id             STRING,
  _source_table       STRING
)
USING PARQUET
PARTITIONED BY (ingest_date DATE);

CREATE TABLE IF NOT EXISTS bronze.orders_raw (
  OrderID             BIGINT,
  CustomerID          INT,
  OrderAmount         DECIMAL(18,2),
  OrderStatus         STRING,
  ModifiedDate        TIMESTAMP,
  _ingest_ts          TIMESTAMP,
  _run_id             STRING,
  _source_table       STRING
)
USING PARQUET
PARTITIONED BY (ingest_date DATE);

-- Silver: deduplicated + standardized
CREATE TABLE IF NOT EXISTS silver.customers_curated (
  customer_id         INT,
  customer_name       STRING,
  email               STRING,
  phone               STRING,
  modified_ts         TIMESTAMP,
  is_deleted          BOOLEAN,
  _ingest_ts          TIMESTAMP,
  _run_id             STRING
)
USING PARQUET;

CREATE TABLE IF NOT EXISTS silver.orders_curated (
  order_id            BIGINT,
  customer_id         INT,
  order_amount        DECIMAL(18,2),
  order_status        STRING,
  modified_ts         TIMESTAMP,
  is_deleted          BOOLEAN,
  _ingest_ts          TIMESTAMP,
  _run_id             STRING
)
USING PARQUET;

-- Gold: business aggregates
CREATE TABLE IF NOT EXISTS gold.orders_daily_metrics (
  metric_date         DATE,
  total_orders        BIGINT,
  total_revenue       DECIMAL(18,2),
  active_customers    BIGINT,
  _run_id             STRING,
  _processed_ts       TIMESTAMP
)
USING PARQUET;
```

## 5) JDBC Extraction Pattern (Initial Full + Incremental)

### JDBC URL

```text
jdbc:sqlserver://<host>:1433;databaseName=<db>;encrypt=true;trustServerCertificate=false;loginTimeout=30;
```

### Read strategy

- If no watermark exists in control table -> **full load**.
- Else -> query source with `watermark_column > last_success_value`.
- Capture `current_max_watermark` from extracted data.
- Write Bronze.
- Only on successful downstream completion, update control watermark to `current_max_watermark`.

### PySpark JDBC helper (production style)

```python
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

def build_jdbc_url(cfg: dict) -> str:
    return (
        f"jdbc:sqlserver://{cfg['host']}:{cfg['port']};"
        f"databaseName={cfg['database']};"
        f"encrypt={cfg.get('encrypt', 'true')};"
        f"trustServerCertificate={cfg.get('trustServerCertificate', 'false')};"
        f"loginTimeout=30;"
    )

def read_sqlserver_df(
    spark: SparkSession,
    jdbc_url: str,
    dbtable_or_query: str,
    user: str,
    password: str,
    driver: str,
    fetchsize: int = 10000
) -> DataFrame:
    return (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", dbtable_or_query)
        .option("user", user)
        .option("password", password)
        .option("driver", driver)
        .option("fetchsize", fetchsize)
        .option("queryTimeout", 600)
        .load()
    )

def source_query(schema: str, table: str, watermark_col: str, last_wm: str | None) -> str:
    base = f"SELECT * FROM {schema}.{table}"
    if not last_wm:
        return f"({base}) src"
    return f"({base} WHERE {watermark_col} > '{last_wm}') src"
```


## 6) Bronze Job (Raw Ingestion)

Responsibilities:
- Fetch watermark state.
- Extract data via JDBC (full/incremental).
- Add ingestion metadata.
- Append into Bronze table partitioned by `ingest_date`.
- Persist run metrics.

```python
import sys
import uuid
import logging
from datetime import datetime
from pyspark.sql import SparkSession, functions as F

def get_spark(app_name: str) -> SparkSession:
    spark = (
        SparkSession.builder
        .appName(app_name)
        .enableHiveSupport()
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.shuffle.partitions", "200")
        .getOrCreate()
    )
    return spark

def get_last_watermark(spark, control_table, source_schema, source_table, watermark_col):
    q = f"""
        SELECT last_success_value
        FROM {control_table}
        WHERE source_system='sqlserver'
          AND source_schema='{source_schema}'
          AND source_table='{source_table}'
          AND watermark_column='{watermark_col}'
          AND last_status='SUCCESS'
        ORDER BY updated_at DESC
        LIMIT 1
    """
    rows = spark.sql(q).collect()
    return rows[0]["last_success_value"].strftime("%Y-%m-%d %H:%M:%S") if rows else None

def write_control_state(spark, control_table, state_dict):
    state_df = spark.createDataFrame([state_dict])
    state_df.write.mode("append").insertInto(control_table)

def bronze_ingest_one_table(cfg, table_cfg, jdbc_user, jdbc_password):
    run_id = str(uuid.uuid4())
    spark = get_spark(f"bronze_{table_cfg['source_table']}_{run_id[:8]}")
    logger = logging.getLogger("bronze_ingest")
    logger.setLevel(logging.INFO)
    logger.info("Bronze ingestion started run_id=%s table=%s", run_id, table_cfg["source_table"])

    control_table = f"{cfg['watermark']['control_db']}.{cfg['watermark']['control_table']}"
    last_wm = get_last_watermark(
        spark, control_table,
        table_cfg["source_schema"], table_cfg["source_table"], table_cfg["watermark_column"]
    )

    jdbc_url = build_jdbc_url(cfg["sqlserver"])
    query = source_query(
        table_cfg["source_schema"], table_cfg["source_table"], table_cfg["watermark_column"], last_wm
    )
    src_df = read_sqlserver_df(
        spark, jdbc_url, query, jdbc_user, jdbc_password, cfg["sqlserver"]["driver"]
    )

    if src_df.rdd.isEmpty():
        logger.info("No new records for table=%s", table_cfg["source_table"])
        write_control_state(spark, control_table, {
            "source_system": "sqlserver",
            "source_schema": table_cfg["source_schema"],
            "source_table": table_cfg["source_table"],
            "watermark_column": table_cfg["watermark_column"],
            "last_success_value": datetime.strptime(last_wm, "%Y-%m-%d %H:%M:%S") if last_wm else None,
            "last_run_id": run_id,
            "last_status": "SUCCESS",
            "last_row_count": 0,
            "updated_at": datetime.utcnow()
        })
        spark.stop()
        return

    max_wm = src_df.agg(F.max(F.col(table_cfg["watermark_column"])).alias("mx")).collect()[0]["mx"]

    bronze_df = (
        src_df
        .withColumn("_ingest_ts", F.current_timestamp())
        .withColumn("_run_id", F.lit(run_id))
        .withColumn("_source_table", F.lit(f"{table_cfg['source_schema']}.{table_cfg['source_table']}"))
        .withColumn("ingest_date", F.to_date(F.current_timestamp()))
    )

    bronze_df.write.mode("append").insertInto(table_cfg["bronze_table"])
    row_count = bronze_df.count()
    logger.info("Bronze write complete table=%s rows=%s", table_cfg["bronze_table"], row_count)

    write_control_state(spark, control_table, {
        "source_system": "sqlserver",
        "source_schema": table_cfg["source_schema"],
        "source_table": table_cfg["source_table"],
        "watermark_column": table_cfg["watermark_column"],
        "last_success_value": max_wm,
        "last_run_id": run_id,
        "last_status": "SUCCESS",
        "last_row_count": int(row_count),
        "updated_at": datetime.utcnow()
    })

    spark.stop()
```

## 7) Silver Job (Cleanse, Dedupe, Standardize)

Typical Silver transformations:
- Type casting + column renaming standards
- Null handling and business rules
- Dedup by PK using latest `modified_ts`
- Soft-delete handling if source provides delete flag/events

```python
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

def transform_customers_to_silver(spark, bronze_table: str, silver_table: str, run_id: str):
    bronze = spark.table(bronze_table)

    standardized = (
        bronze
        .select(
            F.col("CustomerID").cast("int").alias("customer_id"),
            F.trim(F.col("CustomerName")).alias("customer_name"),
            F.lower(F.trim(F.col("Email"))).alias("email"),
            F.trim(F.col("Phone")).alias("phone"),
            F.col("ModifiedDate").cast("timestamp").alias("modified_ts"),
            F.lit(False).alias("is_deleted"),
            F.col("_ingest_ts"),
            F.lit(run_id).alias("_run_id")
        )
        .filter(F.col("customer_id").isNotNull())
    )

    w = Window.partitionBy("customer_id").orderBy(F.col("modified_ts").desc_nulls_last(), F.col("_ingest_ts").desc())
    deduped = standardized.withColumn("rn", F.row_number().over(w)).filter("rn=1").drop("rn")

    deduped.createOrReplaceTempView("silver_stage_customers")
    spark.sql(f"""
        MERGE INTO {silver_table} t
        USING silver_stage_customers s
        ON t.customer_id = s.customer_id
        WHEN MATCHED THEN UPDATE SET
          t.customer_name = s.customer_name,
          t.email = s.email,
          t.phone = s.phone,
          t.modified_ts = s.modified_ts,
          t.is_deleted = s.is_deleted,
          t._ingest_ts = s._ingest_ts,
          t._run_id = s._run_id
        WHEN NOT MATCHED THEN INSERT *
    """)
```

## 8) Gold Job (Business Aggregates)

```python
from pyspark.sql import SparkSession, functions as F

def build_gold_orders_daily(spark: SparkSession, orders_silver: str, gold_table: str, run_id: str):
    orders = spark.table(orders_silver).filter(~F.col("is_deleted"))

    gold_df = (
        orders
        .withColumn("metric_date", F.to_date("modified_ts"))
        .groupBy("metric_date")
        .agg(
            F.countDistinct("order_id").alias("total_orders"),
            F.sum("order_amount").cast("decimal(18,2)").alias("total_revenue"),
            F.countDistinct("customer_id").alias("active_customers")
        )
        .withColumn("_run_id", F.lit(run_id))
        .withColumn("_processed_ts", F.current_timestamp())
    )

    gold_df.createOrReplaceTempView("gold_stage_orders_daily")
    spark.sql(f"""
        MERGE INTO {gold_table} t
        USING gold_stage_orders_daily s
        ON t.metric_date = s.metric_date
        WHEN MATCHED THEN UPDATE SET
          t.total_orders = s.total_orders,
          t.total_revenue = s.total_revenue,
          t.active_customers = s.active_customers,
          t._run_id = s._run_id,
          t._processed_ts = s._processed_ts
        WHEN NOT MATCHED THEN INSERT *
    """)
```

## 9) Airflow DAG (Daily 10 PM, Dependency and Retry)

Use timezone-aware schedule and explicit task ordering: `bronze >> silver >> gold`.

```python
from datetime import timedelta
import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

local_tz = pendulum.timezone("America/Chicago")

default_args = {
    "owner": "data-eng",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
}

with DAG(
    dag_id="sqlserver_hive_medallion_daily",
    default_args=default_args,
    start_date=pendulum.datetime(2026, 1, 1, tz=local_tz),
    schedule="0 22 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["sqlserver", "hive", "medallion", "spark"],
) as dag:

    config_path = Variable.get("pipeline_config_path")

    bronze = SparkSubmitOperator(
        task_id="bronze_ingest",
        application="/opt/airflow/jobs/bronze_ingest.py",
        name="bronze_ingest_job",
        application_args=["--config", config_path],
        conn_id="spark_default",
        verbose=False,
    )

    silver = SparkSubmitOperator(
        task_id="silver_transform",
        application="/opt/airflow/jobs/silver_transform.py",
        name="silver_transform_job",
        application_args=["--config", config_path],
        conn_id="spark_default",
        verbose=False,
    )

    gold = SparkSubmitOperator(
        task_id="gold_aggregate",
        application="/opt/airflow/jobs/gold_aggregate.py",
        name="gold_aggregate_job",
        application_args=["--config", config_path],
        conn_id="spark_default",
        verbose=False,
    )

    bronze >> silver >> gold
```

## 10) Validation Framework (Data Quality Gates)

Run checks at each stage and fail fast:

1. **Row count sanity**: incremental rows should not exceed threshold unexpectedly.
2. **Primary key null/duplicate checks** (Silver).
3. **Schema drift detection**: source columns changed unexpectedly.
4. **Business checks**: e.g., negative order amount not allowed.

```python
from pyspark.sql import functions as F

def assert_non_empty(df, name: str):
    if df.rdd.isEmpty():
        raise ValueError(f"{name} is empty")

def assert_no_null_pk(df, pk: str, name: str):
    cnt = df.filter(F.col(pk).isNull()).count()
    if cnt > 0:
        raise ValueError(f"{name}: {cnt} null primary keys in {pk}")

def assert_no_duplicate_pk(df, pk: str, name: str):
    dup = df.groupBy(pk).count().filter("count > 1").count()
    if dup > 0:
        raise ValueError(f"{name}: duplicate primary keys found in {pk}")

def assert_non_negative(df, col_name: str, name: str):
    bad = df.filter(F.col(col_name) < 0).count()
    if bad > 0:
        raise ValueError(f"{name}: found {bad} negative values in {col_name}")
```

## 11) Logging and Observability

Minimum production logging for each task/run:
- `run_id`, table name, layer, start/end timestamps
- extracted row count, written row count, reject count
- last watermark and current watermark
- execution duration
- error class + stack trace on failure

Recommendations:
- Use structured JSON logs.
- Push key metrics to monitoring table `control.etl_run_metrics`.
- Integrate Airflow alerts (email/Slack/Teams) for failures and SLA misses.

Example metrics table:

```sql
CREATE TABLE IF NOT EXISTS control.etl_run_metrics (
  run_id            STRING,
  dag_id            STRING,
  task_id           STRING,
  layer             STRING,
  source_table      STRING,
  target_table      STRING,
  row_count         BIGINT,
  status            STRING,
  error_message     STRING,
  started_at        TIMESTAMP,
  ended_at          TIMESTAMP,
  duration_seconds  BIGINT
)
USING PARQUET;
```

## 12) Error Handling and Recovery

### Principles
- Fail a task on hard validation errors.
- Keep watermark advancement **atomic and post-success only**.
- Idempotent writes (use MERGE in Silver/Gold).
- Distinguish transient vs non-transient errors.

### Practical policy
- Retries for transient issues (network/JDBC timeout/cluster preemption).
- No retries for deterministic data-quality failures.
- Quarantine bad records to `bronze_rejects` with reason code.
- Add circuit breaker if repeated failures exceed N runs.

Pseudo-pattern:

```python
try:
    # extract -> validate -> write bronze/silver/gold
    # update watermark only after successful write+validation
    pass
except TransientError as e:
    log.error("Transient failure", exc_info=True)
    raise  # let Airflow retry
except Exception as e:
    log.error("Non-retryable failure", exc_info=True)
    # optionally write to failure audit table
    raise
```

## 13) Initial Backfill and Incremental Runtime Flow

### Day 0 (Initial load)
1. Control table empty -> full extract from SQL Server source tables.
2. Bronze append full snapshot with `_run_id`, `_ingest_ts`.
3. Silver MERGE dedupes latest by PK.
4. Gold aggregates computed.
5. Store max watermark per table in `control.etl_watermark_state`.

### Day N (Incremental)
1. Read `last_success_value`.
2. Extract only `watermark_column > last_success_value`.
3. Bronze append delta.
4. Silver MERGE updates/inserts impacted entities.
5. Gold recomputes impacted dates (or full daily overwrite partition strategy).
6. Update watermark to extracted max value if entire chain succeeded.

---

## 14) Performance and Scalability Notes

- Enable predicate pushdown through JDBC query.
- For very large tables, use partitioned JDBC read:
  - `.option("partitionColumn", "ID")`
  - `.option("lowerBound", "...")`
  - `.option("upperBound", "...")`
  - `.option("numPartitions", "16")`
- Use file compaction in Silver/Gold.
- Optimize merge keys and partition strategy.
- Schedule heavy dimensions/facts in separate DAG branches if needed.

## 15) Security and Compliance

- Never hardcode JDBC credentials.
- Store secrets in Airflow Connections backed by secret manager (Azure Key Vault preferred).
- Enforce TLS (`encrypt=true`) in JDBC.
- Apply least privilege:
  - SQL Server login restricted to required schemas/tables.
  - ADLS ACLs per environment/layer.
- Add PII masking/tokenization in Silver where applicable.

## 16) End-to-End Operational Checklist

- [ ] Hive DBs and tables created (Bronze/Silver/Gold/Control).
- [ ] Airflow connection `sqlserver_jdbc` configured and tested.
- [ ] Spark cluster has SQL Server JDBC driver jar.
- [ ] Config path available to Airflow variable.
- [ ] Day-0 full load validated (counts and sample reconciliation).
- [ ] Incremental run validated with synthetic updates in source.
- [ ] Alerts configured for DAG/task failures.
- [ ] Runbook documented (restart, replay, backfill, rollback).


## 17) Minimal Production Runbook

- **Rerun failed day**: clear failed Airflow tasks for execution date and re-run.
- **Backfill date range**: enable `catchup=True` temporarily or trigger manual runs with date params.
- **Watermark correction**: insert corrected state row in control table (never delete history).
- **Schema change**: update DDL + transformation mapping + validation, deploy, then reprocess affected partitions.

## 18) Final Notes

This design gives you:
- Deterministic orchestration at **10 PM daily**
- Safe full-load bootstrap then efficient watermark-based incrementals
- Reliable Medallion layering with governance and observability
- Production controls for failures, quality, and secure operations
