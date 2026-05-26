from __future__ import annotations

from pyspark.sql import SparkSession


def ensure_databases(spark: SparkSession, hive_db: str) -> None:
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {hive_db}")
    spark.sql("CREATE DATABASE IF NOT EXISTS control")


def create_bronze_table(spark: SparkSession, table_name: str) -> None:
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
          payload STRING,
          _ingest_ts TIMESTAMP,
          _run_id STRING,
          _source_table STRING,
          source_file STRING
        )
        USING PARQUET
        PARTITIONED BY (ingest_date DATE)
        """
    )


def create_silver_table(spark: SparkSession, table_name: str) -> None:
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
          Age INT,
          Attrition STRING,
          BusinessTravel STRING,
          DailyRate INT,
          Department STRING,
          DistanceFromHome INT,
          Education INT,
          EducationField STRING,
          EmployeeCount INT,
          EmployeeNumber INT,
          EnvironmentSatisfaction INT,
          Gender STRING,
          HourlyRate INT,
          JobInvolvement INT,
          JobLevel INT,
          JobRole STRING,
          JobSatisfaction INT,
          MaritalStatus STRING,
          MonthlyIncome INT,
          MonthlyRate INT,
          NumCompaniesWorked INT,
          Over18 STRING,
          OverTime STRING,
          PercentSalaryHike INT,
          PerformanceRating INT,
          RelationshipSatisfaction INT,
          StandardHours INT,
          StockOptionLevel INT,
          TotalWorkingYears INT,
          TrainingTimesLastYear INT,
          WorkLifeBalance INT,
          YearsAtCompany INT,
          YearsInCurrentRole INT,
          YearsSinceLastPromotion INT,
          YearsWithCurrManager INT,
          Load_date TIMESTAMP,
          _run_id STRING
        )
        USING PARQUET
        """
    )


def create_gold_table(spark: SparkSession, table_name: str) -> None:
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
          Load_date DATE,
          Department STRING,
          Attrition STRING,
          Gender STRING,
          employee_count BIGINT,
          avg_monthly_income DOUBLE,
          avg_years_at_company DOUBLE,
          _run_id STRING,
          _processed_ts TIMESTAMP
        )
        USING PARQUET
        PARTITIONED BY (Load_date)
        """
    )


def ensure_control_table(spark: SparkSession, control_db: str, control_table: str) -> None:
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {control_db}.{control_table} (
          environment STRING,
          source_system STRING,
          source_schema STRING,
          source_table STRING,
          watermark_column STRING,
          last_success_value TIMESTAMP,
          last_run_id STRING,
          last_status STRING,
          last_row_count BIGINT,
          updated_at TIMESTAMP
        )
        USING PARQUET
        """
    )
