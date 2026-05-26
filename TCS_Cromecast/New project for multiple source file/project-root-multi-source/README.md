# Multi-Source Medallion Data Pipeline (Spark + Airflow + S3)

## English
This project is a complete, production-style, multi-file data pipeline using:
- **PySpark/Spark** for processing
- **Airflow TaskFlow API** for orchestration
- **S3** for Bronze/Silver/Gold (Parquet)
- **SQL Server + CSV + JSON** as supported source types
- **DEV -> UAT -> PROD** environment progression

### Implemented Requirements
- Separate pipelines for `csv`, `json`, `table`
- Source can be local/S3/JDBC based on config
- Daily schedule at **10 PM** (`0 22 * * *`)
- File freshness validation (24h). If stale/not available:
  - prints **`File is not available to process`**
- Transformations:
  - drop duplicates
  - fill nulls with 0
  - select required columns
  - add `Load_date` watermark column
- Load strategy:
  - Full load initially (`--load-type full`)
  - Incremental afterwards (`--load-type incremental`) via watermark
- Watermark stored on S3 (JSON under watermark path)
- Medallion output paths:
  - `s3://bucket/bronze/{source_type}/{table_name}/`
  - `s3://bucket/silver/{source_type}/{table_name}/`
  - `s3://bucket/gold/{source_type}/{table_name}/`

### Project Structure
`project-root-multi-source/` includes all required files and folders exactly as requested.

### Setup
1. Install Python dependencies:
   - `pyspark`
   - `boto3`
   - `pyyaml`
   - `apache-airflow`
2. Configure AWS credentials and source locations in:
   - `conf/dev.yaml`
   - `conf/uat.yaml`
   - `conf/prod.yaml`
3. For SQL Server source:
   - Run `sql/create_tables.sql`
   - Ensure JDBC driver is available to Spark classpath.
4. Place DAGs in Airflow DAG folder or mount project path.

### Run Jobs Manually
- CSV full load:
  - `python jobs/csv/bronze_ingest.py --env dev --source-type csv --load-type full`
  - `python jobs/csv/silver_transform.py --env dev --source-type csv --load-type full`
  - `python jobs/csv/gold_aggregate.py --env dev --source-type csv --load-type full`
- JSON incremental:
  - `python jobs/json/bronze_ingest.py --env dev --source-type json --load-type incremental`
  - `python jobs/json/silver_transform.py --env dev --source-type json --load-type incremental`
  - `python jobs/json/gold_aggregate.py --env dev --source-type json --load-type incremental`
- TABLE incremental:
  - `python jobs/table/bronze_ingest.py --env dev --source-type table --load-type incremental`
  - `python jobs/table/silver_transform.py --env dev --source-type table --load-type incremental`
  - `python jobs/table/gold_aggregate.py --env dev --source-type table --load-type incremental`

### Airflow DAGs
- `dags/csv_medallion_dag.py`
- `dags/json_medallion_dag.py`
- `dags/table_medallion_dag.py`

Each DAG uses decorator-based `@dag` + `@task` (TaskFlow API) and executes bronze -> silver -> gold.

### Environment Promotion (DEV -> UAT -> PROD)
1. Validate in DEV (`--env dev`)
2. Promote to UAT (`--env uat`) after validation
3. Promote to PROD (`--env prod`) post sign-off

### Notes
- Azure is included in architecture context; destination remains S3 as required.
- S3 source freshness check is active for S3-based files.
- Watermark path is environment-aware.

---

## Hindi (Hindi + English mix for practical usage)
Ye project ek **complete multi-source medallion pipeline** hai jisme:
- Source: CSV, JSON, SQL table
- Processing: Spark/PySpark
- Orchestration: Airflow TaskFlow
- Destination: **S3 only** (Bronze/Silver/Gold Parquet)

### Kaise kaam karta hai
1. Bronze ingest job source data read karti hai.
2. Agar source file stale ho (24 ghante se update nahi), to job skip karegi aur print karegi:
   - `File is not available to process`
3. Silver transform:
   - duplicates remove
   - null values ko 0
   - selected columns only
   - `Load_date` add
4. Gold aggregate:
   - business-level summary create
5. Watermark S3 par store hota hai incremental load ke liye.

### Daily Schedule
- Har din **10 PM** par DAG trigger hota hai (`0 22 * * *`)

### Folder Separation
- `jobs/csv/*` alag
- `jobs/json/*` alag
- `jobs/table/*` alag
- Common logic `jobs/common/*` me shared hai

### DEV/UAT/PROD process
- Pehle DEV me run/test karo
- Fir UAT me promote karo
- Fir PROD me deploy karo

### Important config keys
- `source.type`
- `source.location` (`location_type`)
- `destination.s3_bucket`
- `destination.s3_prefix`

Aap conf files me values update karke direct run kar sakte ho.