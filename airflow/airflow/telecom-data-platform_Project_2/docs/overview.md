# Telecom Data Platform Overview

## Architecture

- Airflow orchestrates the pipeline lifecycle.
- Notebooks contain ETL business logic and reusable utilities.
- SQL assets define the warehouse schema and operational loads.
- Tests validate critical transformation behavior.

## Pipeline Flow

1. `customer_pipeline_dag.py`: extract -> transform -> load -> summary.
2. `customer_etl.py`: enriches raw customer data and writes normalized output.
3. SQL directory stores DDL, DML, and stored procedure artifacts for deployment.
