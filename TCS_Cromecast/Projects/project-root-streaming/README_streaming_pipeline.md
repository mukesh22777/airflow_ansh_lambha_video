# Streaming Medallion Data Pipeline (Spark + Airflow + Hive Parquet + Azure-ready)

## 1) Project Overview (English)
- This project implements a near real-time (2-hourly) medallion pipeline using `PySpark`, `Hive`, and `Airflow TaskFlow API`.
- Source supports `CSV` or `JSON` (config-driven), and data lands into Hive Parquet tables:
  - `Train_bronze`
  - `Train_silver`
  - `Train_gold`
- Incremental processing is watermark-only (`Load_date`), with environment-aware state for `DEV/UAT/PROD`.

## 2) परियोजना का सार (Hindi)
- यह प्रोजेक्ट 2 घंटे के अंतराल पर चलने वाला near streaming medallion pipeline है।
- Source `CSV` या `JSON` हो सकता है (config से नियंत्रित)।
- Data Hive Parquet tables में लोड होता है: `Train_bronze`, `Train_silver`, `Train_gold`।
- Incremental load केवल watermark (`Load_date`) के आधार पर चलता है।

## 3) Architecture / आर्किटेक्चर
- **Bronze**: raw ingestion with freshness check + `Load_date`
- **Silver**: drop duplicates, fill NA with 0, select columns, standardize schema
- **Gold**: business aggregations (`employee_count`, salary metrics) + watermark update

## 4) Folder Structure
```text
project-root-streaming/
├── sample_data/sample.csv
├── dags/streaming_medallion_dag.py
├── jobs/bronze_ingest.py
├── jobs/silver_transform.py
├── jobs/gold_aggregate.py
├── jobs/common/config.py
├── jobs/common/logger.py
├── jobs/common/watermark.py
├── jobs/common/validation.py
├── jobs/common/hive_ddl.py
├── conf/dev.yaml
├── conf/uat.yaml
├── conf/prod.yaml
├── sql/hive_ddl.sql
├── logs/.gitkeep
├── .gitignore
└── README_streaming_pipeline.md
```

## 5) Core Business Rules / मुख्य नियम
1. DAG schedule: `0 */2 * * *`
2. `max_active_runs=1` to prevent overlap
3. SLA: each task has `90 minutes`
4. Source freshness: if source file not updated in last 2 hours, task skips with message:
   - `File is not available to process`
5. Incremental load: watermark-only by `Load_date` state file
6. Source type: CSV/JSON configurable in `conf/*.yaml`

## 6) Transformations Implemented
- Drop duplicates (`id`, `timestamp`)
- Fill nulls with `0`
- Select curated columns
- Add/refresh `Load_date` watermark column
- Gold-level aggregation by `department, city`

## 7) Environments (DEV -> UAT -> PROD)
- Separate config files:
  - `conf/dev.yaml`
  - `conf/uat.yaml`
  - `conf/prod.yaml`
- Separate Hive DB and watermark state per environment.

## 8) How to Run (English + Hindi)
### Prerequisites
- Python 3.9+
- `pyspark`, `pyyaml`, `apache-airflow`
- Hive metastore enabled via Spark Hive support

### Install (example)
```bash
pip install pyspark pyyaml apache-airflow
```

### Local job run (DEV)
```bash
python jobs/bronze_ingest.py --env dev
python jobs/silver_transform.py --env dev
python jobs/gold_aggregate.py --env dev
```

### Airflow DAG deploy
- Copy DAG to Airflow DAGs folder or mount this `dags/` directory.
- Ensure project root is in `PYTHONPATH` for job imports.

## 9) Azure Notes / Azure पर चलाने के लिए
- Current code is Azure-ready by configuration pattern.
- In cloud deployment:
  - Source path can be `abfss://...` for ADLS Gen2.
  - Hive/Spark metastore can be externalized (Databricks/Synapse/Hive Metastore).
  - Keep env-specific secrets in Key Vault / secret scope (not in YAML).

## 10) Concurrency Control & Small-file Compaction
- **Concurrency control**:
  - Airflow `max_active_runs=1`
  - Single DAG chain `bronze >> silver >> gold`
  - Environment-specific watermark file avoids cross-env collision
- **Small-file compaction**:
  - For high-frequency streaming, add periodic compaction job:
    - Read table partitions
    - `repartition/coalesce` to target file size (e.g., 128MB)
    - Write back with overwrite-by-partition
  - Tune `spark.sql.shuffle.partitions` per environment

## 11) Validation and Error Handling
- Column validation before transformations
- Source format validation (`csv/json`)
- Freshness guard to skip stale source safely
- Structured logging to `logs/pipeline.log`

## 12) Watermark Behavior
- Watermark stored in `state/watermark_<env>.json`
- Updated only after successful Gold load
- Bronze checks source freshness and source modified time against watermark

## 13) SQL DDL
- `sql/hive_ddl.sql` contains Parquet DDL for:
  - `Train_bronze`
  - `Train_silver`
  - `Train_gold`

## 14) Git Usage
- Initialize and push to Git repository:
```bash
git init
git add .
git commit -m "Streaming medallion pipeline with Spark + Airflow + Hive Parquet"
```

---
If you want, I can also add a `requirements.txt`, Airflow `Docker Compose`, and unit tests for validations/watermark logic.
