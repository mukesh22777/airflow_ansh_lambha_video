# Telecom Data Platform

This sample enterprise-style project contains a telecom customer ETL pipeline with Airflow orchestration, Databricks/Notebook patterns, and SQL artifacts.

## Structure

- `airflow/`
  - `dags/customer_pipeline_dag.py` — Airflow DAG that orchestrates extract, transform, and load tasks.
  - `plugins/` — placeholder for custom Airflow operators and hooks.
  - `requirements.txt` — Airflow dependencies.
- `databricks/` — Databricks notebook assets and deployment artifacts.
- `notebooks/`
  - `customer_etl.py` — standalone ETL notebook-style Python script.
  - `common_utils.py` — shared utility methods.
  - `configs/` — environment and pipeline configuration.
- `sql/` — schema, data load, and stored procedure definitions.
- `tests/` — unit tests for ETL logic.
- `docs/` — architecture and deployment guidance.

## Getting Started

Use Airflow to deploy the DAG and execute the customer pipeline. The notebook and SQL files show how business logic is organized for a telecom enterprise data platform.
