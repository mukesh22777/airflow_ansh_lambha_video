# SQL Server -> Hive Medallion on Azure with Airflow + PySpark

This repository implements a production-oriented Medallion data pipeline:

- **Source**: SQL Server (Azure SQL / SQL Server)
- **Destination**: Hive tables on Azure storage
- **Layers**: Bronze (raw), Silver (curated), Gold (aggregates)
- **Orchestration**: Airflow DAG scheduled **daily at 10:00 PM** (`0 22 * * *`)
- **Load mode**: Initial full load, then incremental by `modified_date` watermark

## Architecture

1. `dags/sqlserver_hive_medallion_dag.py` triggers three Spark jobs in sequence.
2. `jobs/bronze_ingest.py` ingests source data from SQL Server into Bronze.
3. `jobs/silver_transform.py` cleans, deduplicates, and standardizes into Silver.
4. `jobs/gold_aggregate.py` computes business aggregates and updates watermark state.
5. `jobs/common/` provides shared utilities:
   - config loading and Spark session bootstrap
   - logging
   - JDBC extraction
   - watermark state handling
   - validation checks
   - Hive DDL creation helpers

## Runtime Flow

1. Bronze reads table config from YAML.
2. For each source table:
   - if watermark exists, read rows with `watermark_column > last_success_value`
   - else perform full load
3. Bronze writes to partitioned Hive Bronze tables with ingestion metadata.
4. Silver reads current `run_id` rows from Bronze and writes curated records.
5. Gold computes daily metrics and writes Gold output.
6. Gold updates control watermark table only after successful processing.

## Prerequisites

- Airflow 2.x
- Spark 3.x with Hive support
- SQL Server JDBC driver available to Spark (`com.microsoft.sqlserver.jdbc.SQLServerDriver`)
- Python packages:
  - `PyYAML`
  - `pyspark`

## Configuration

Main config file: `conf/pipeline_config.yaml`

Contains:
- schedule and retry policy
- Spark tuning
- SQL Server connection settings
- Azure paths
- watermark control table details
- per-table mapping (source and Bronze/Silver/Gold targets)

> Credentials should not be stored in YAML. Provide SQL credentials via environment variables:
> - `SQLSERVER_USER`
> - `SQLSERVER_PASSWORD`

## Running Jobs Manually

```bash
spark-submit jobs/bronze_ingest.py --config conf/pipeline_config.yaml --run-id manual_20260504
spark-submit jobs/silver_transform.py --config conf/pipeline_config.yaml --run-id manual_20260504
spark-submit jobs/gold_aggregate.py --config conf/pipeline_config.yaml --run-id manual_20260504
```

## Airflow Usage

Set variable `pipeline_config_path` to an absolute path of `pipeline_config.yaml` or rely on the default path used in DAG.

Enable DAG `sqlserver_hive_medallion_dag`.

## Observability and Reliability

- Structured logging across all jobs
- Data quality checks (required columns, duplicate keys, null checks)
- Fail-fast error handling with clear exceptions
- Watermark updated only in Gold step after successful upstream processing

## Notes

- Pipeline is table-driven via YAML and supports extending to additional tables.
- Bronze and Gold tables are partitioned by date in Hive DDL.