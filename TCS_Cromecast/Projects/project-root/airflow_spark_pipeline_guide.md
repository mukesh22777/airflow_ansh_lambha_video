# Airflow + Spark Medallion Pipeline Guide (Hindi + English)

## 1) Project Overview / परियोजना का उद्देश्य
- Yeh pipeline `Train.csv` (ya SQL Server table) ko read karke Hive mein 3 layers banati hai:
  - `Train_bronze` (raw)
  - `Train_silver` (cleaned)
  - `Train_gold` (aggregated)
- Architecture: **Medallion (Bronze -> Silver -> Gold)**
- Orchestration: **Airflow 2.x TaskFlow API** (`@dag`, `@task`)
- Frequency: **Daily 10 PM** (`0 22 * * *`)
- Load pattern:
  - First run: **Full load**
  - Next runs: **Incremental** using watermark column `Load_date`

## 2) File Structure (Exact)
- `dags/sqlserver_hive_medallion_dag.py`
- `jobs/bronze_ingest.py`
- `jobs/silver_transform.py`
- `jobs/gold_aggregate.py`
- `jobs/common/config.py`
- `jobs/common/logger.py`
- `jobs/common/jdbc.py`
- `jobs/common/watermark.py`
- `jobs/common/validation.py`
- `jobs/common/hive_ddl.py`
- `conf/dev.yaml`
- `conf/uat.yaml`
- `conf/prod.yaml`
- `sql/hive_ddl.sql`
- `logs/.gitkeep`
- `.gitignore`
- `airflow_spark_pipeline_guide.md`

## 3) DAG Flow (TaskFlow API) / DAG ka flow
`sqlserver_hive_medallion_dag.py` me tasks ka order:
1. `check_file_availability` **(FIRST TASK mandatory)**  
   - File exist + fresh check (max age config se).
   - Agar fail, exact message print:  
     **`File is not available to process`**
   - Pipeline stop ho jati hai.
2. `run_bronze_ingest`
3. `run_silver_transform`
4. `run_gold_aggregate`

## 4) Bronze Layer / Raw Ingestion
File: `jobs/bronze_ingest.py`
- Source mode support:
  - `file` (Train.csv)
  - `sqlserver` (JDBC)
- Expected schema columns verify karta hai:
  - Age, Attrition, BusinessTravel, DailyRate, Department, DistanceFromHome, Education, EducationField, EmployeeCount, EmployeeNumber, EnvironmentSatisfaction, Gender, HourlyRate, JobInvolvement, JobLevel, JobRole, JobSatisfaction, MaritalStatus, MonthlyIncome, MonthlyRate, NumCompaniesWorked, Over18, OverTime, PercentSalaryHike, PerformanceRating, RelationshipSatisfaction, StandardHours, StockOptionLevel, TotalWorkingYears, TrainingTimesLastYear, WorkLifeBalance, YearsAtCompany, YearsInCurrentRole, YearsSinceLastPromotion, YearsWithCurrManager
- `Load_date` watermark column add karta hai.
- Full vs incremental:
  - No previous watermark -> full load
  - Previous watermark exists -> `Load_date > last_success_value`
- Data JSON payload ke form me Bronze table me append hota hai (Parquet Hive table).

## 5) Silver Layer / Cleansing
File: `jobs/silver_transform.py`
- Bronze payload parse karta hai into typed columns.
- Required transformations:
  - `dropDuplicates()`
  - `fillna(0)`
  - relevant columns selection only
  - `Load_date` preserve as watermark
- Output: `Train_silver` as Parquet Hive table.

## 6) Gold Layer / Aggregation
File: `jobs/gold_aggregate.py`
- Silver se grouped metrics banata hai by:
  - `Load_date`, `Department`, `Attrition`, `Gender`
- Metrics:
  - employee_count
  - avg_monthly_income
  - avg_years_at_company
- Output: `Train_gold` (Parquet, partitioned by `Load_date`)
- Success ke baad watermark table update hota hai.

## 7) Environment Strategy (DEV -> UAT -> PROD)
Config files:
- `conf/dev.yaml`
- `conf/uat.yaml`
- `conf/prod.yaml`

Har environment me:
- source settings
- SQL Server settings
- hive db/table names
- watermark control info
- spark tuning

Run order recommended:
1. DEV validation
2. UAT sign-off
3. PROD deployment

## 8) Hive DDL
File: `sql/hive_ddl.sql`
- Creates:
  - `hr_analytics.Train_bronze`
  - `hr_analytics.Train_silver`
  - `hr_analytics.Train_gold`
  - `control.etl_watermark_state`
- All destination tables are `USING PARQUET`.

## 9) Airflow Variables / Runtime Controls
Set these in Airflow:
- `pipeline_env` -> `dev` / `uat` / `prod`
- `pipeline_config_path` -> absolute path of matching YAML

## 10) Execution Commands / Chalane ke commands
Manual job runs (example):
```bash
spark-submit jobs/bronze_ingest.py --config conf/dev.yaml --run-id 20260505220000 --env dev
spark-submit jobs/silver_transform.py --config conf/dev.yaml --run-id 20260505220000 --env dev
spark-submit jobs/gold_aggregate.py --config conf/dev.yaml --run-id 20260505220000 --env dev
```

## 11) Error Handling & Logging
- Common logger module se structured logs.
- Validation module se schema/required checks.
- Failures raise exceptions for Airflow retry/failure tracking.
- If file not available, DAG stops safely before Spark jobs.

## 12) Git + Azure Best Practices
- Secrets ko YAML me plain text me na rakhein.
- CI/CD me DEV/UAT/PROD promotion gates rakhein.
- Bronze immutable rakhein, Silver curated, Gold business-ready.
- Branch protection + PR review enforce karein.

## 13) Quick Checklist / Fast validation
- [ ] `check_file_availability` first task hai
- [ ] DAG decorators use ho rahe hain (`@dag`, `@task`)
- [ ] Silver me dedup + fillna(0) + relevant columns + `Load_date`
- [ ] Full + incremental watermark logic hai
- [ ] Schedule `0 22 * * *` hai
- [ ] DEV/UAT/PROD configs present hain
- [ ] Destination Hive tables Parquet format me hain
